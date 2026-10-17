"""A partitioned pipeline whose input never mentions the partition reads identical input every time: load-time guards per
source kind, and {partition_start}/{partition_end} + safe quoting for `database` sources (same semantics as
context_engineering_pipeline's warehouse_query). Real materializations against a real sqlite file."""
import logging
import sqlite3

import dagster as dg
import pytest

SUMM = {"id": "summ", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "Rows: {text}"}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def comp(mod, source, steps=None, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2026-01-01")
    return mod.AgenticPipelineComponent(asset_name_prefix="p", source=source, steps=steps or [dict(SUMM)],
                                        outputs={"assets": ["summ"]}, **kw)


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "t.db"
    con = sqlite3.connect(path)
    con.execute("create table ev (day text, name text)")
    con.executemany("insert into ev values (?, ?)", [
        ("2026-01-01", "one"), ("2026-01-02", "two"), ("2026-01-02", "O'Brien: x"), ("2026-01-03", "three")])
    con.commit()
    con.close()
    return f"sqlite:///{path}"


def prompt_of(fake_litellm):
    return fake_litellm.calls[-1]["messages"][-1]["content"]


def mat(c, key):
    r = dg.materialize(c.build_defs(None).assets, partition_key=key, raise_on_error=False)
    assert r.success, [e.event_specific_data.error.message for e in r.get_step_failure_events()]


def test_database_query_is_bound_to_the_partition_window_and_key(mod, fake_litellm, db):
    q = "select name from ev where day >= substr({partition_start}, 1, 10) and day < substr({partition_end}, 1, 10) order by name"
    c = comp(mod, {"kind": "database", "database_url": db, "query": q})
    mat(c, "2026-01-02")
    assert "O'Brien: x" in prompt_of(fake_litellm) and "two" in prompt_of(fake_litellm)
    assert "one" not in prompt_of(fake_litellm) and "three" not in prompt_of(fake_litellm)
    mat(comp(mod, {"kind": "database", "database_url": db, "query": "select name from ev where day = {partition_key}"}), "2026-01-03")
    assert "three" in prompt_of(fake_litellm) and "two" not in prompt_of(fake_litellm)


def test_bound_values_are_quoted_not_spliced(mod, db):
    sql = mod._bind_source_sql("select * from t where k = {partition_key} and a = {partition.a}", "x' OR 1=1 --", {"a": "b:c"}, None)
    assert sql == "select * from t where k = 'x'' OR 1=1 --' and a = 'b\\:c'"
    assert mod._bind_source_sql("select '{{literal}}'", None, None, None) == "select '{literal}'"   # brace escape
    with pytest.raises(ValueError, match="remove the quotes"):
        mod._bind_source_sql("select * from t where k = '{partition_key}'", "a", None, None)
    with pytest.raises(ValueError, match="not available"):
        mod._bind_source_sql("select {partition_start}", "a", None, None)


def test_a_key_with_a_colon_survives_sqlalchemy_text(mod, fake_litellm, db):
    c = comp(mod, {"kind": "database", "database_url": db, "query": "select {partition_key} as k"},
             partition_type="static", partition_values="a:b")
    mat(c, "a:b")
    assert "a:b" in prompt_of(fake_litellm)


def test_partition_start_needs_a_time_window_partition(mod):
    with pytest.raises(ValueError, match="need a daily/weekly/monthly/hourly"):
        comp(mod, {"kind": "database", "database_url": "sqlite://", "query": "select {partition_start}"},
             partition_type="static", partition_values="a,b").build_defs(None)
    comp(mod, {"kind": "database", "database_url": "sqlite://", "query": "select {partition_start}"}).build_defs(None)


def test_database_query_placeholders_without_partitioning_are_rejected(mod):
    with pytest.raises(ValueError, match="no partitioning is configured"):
        mod.AgenticPipelineComponent(
            asset_name_prefix="p", source={"kind": "database", "database_url": "sqlite://", "query": "select {partition_key}"},
            steps=[dict(SUMM)], outputs={"assets": ["summ"]}).build_defs(None)


def test_literal_source_without_placeholder_raises_unless_a_step_uses_the_partition(mod):
    with pytest.raises(ValueError, match="identical input"):
        comp(mod, {"kind": "literal", "text": "fixed"}).build_defs(None)
    comp(mod, {"kind": "literal", "text": "fixed {partition_key}"}).build_defs(None)
    comp(mod, {"kind": "literal", "text": "fixed {partition_start}"}).build_defs(None)
    comp(mod, {"kind": "literal", "text": "fixed"},
         steps=[{**SUMM, "prompt_template": "For {partition_key}: {text}"}]).build_defs(None)      # partition drives the prompt
    # an approval key / sink path using the partition is NOT input
    with pytest.raises(ValueError, match="identical input"):
        comp(mod, {"kind": "literal", "text": "fixed"},
             steps=[dict(SUMM), {"id": "g", "op": "approve", "approval_dir": "x", "key": "{partition_key}"}]).build_defs(None)
    # unpartitioned: nothing to check
    mod.AgenticPipelineComponent(asset_name_prefix="u", source={"kind": "literal", "text": "fixed"}, steps=[dict(SUMM)],
                                 outputs={"assets": ["summ"]}).build_defs(None)


@pytest.mark.parametrize("source,field", [
    ({"kind": "file", "path": "data/latest.txt"}, "path"),
    ({"kind": "url", "url": "https://example.com/latest"}, "url"),
    ({"kind": "database", "database_url": "sqlite://", "query": "select 1"}, "query"),
])
def test_file_url_database_without_placeholder_warn_at_load(mod, source, field, caplog):
    with caplog.at_level(logging.WARNING):
        comp(mod, source).build_defs(None)
    msgs = [r.getMessage() for r in caplog.records if "SAME input" in r.getMessage()]
    assert len(msgs) == 1 and f"source.{field}" in msgs[0] and f"kind={source['kind']}" in msgs[0]


def test_no_warning_when_templated_or_upstream_or_unpartitioned(mod, caplog):
    with caplog.at_level(logging.WARNING):
        comp(mod, {"kind": "file", "path": "data/{partition_key}.txt"}).build_defs(None)
        comp(mod, {"kind": "url", "url": "https://x/?from={partition_start}&to={partition_end}"}).build_defs(None)
        comp(mod, {"kind": "database", "database_url": "sqlite://", "query": "select {partition_key}"}).build_defs(None)
        comp(mod, {"kind": "upstream_asset", "upstream_asset_key": "raw"}).build_defs(None)
        comp(mod, {"kind": "file", "path": "static.txt"},
             steps=[{**SUMM, "prompt_template": "{partition_key}: {text}"}]).build_defs(None)
        mod.AgenticPipelineComponent(asset_name_prefix="u", source={"kind": "file", "path": "x"}, steps=[dict(SUMM)],
                                     outputs={"assets": ["summ"]}).build_defs(None)
    assert not [r for r in caplog.records if "SAME input" in r.getMessage()]


def test_file_source_gets_the_window_substituted(mod, fake_litellm, tmp_path):
    (tmp_path / "2026-01-02T00:00:00+00:00.txt").write_text("window file")
    mat(comp(mod, {"kind": "file", "path": str(tmp_path / "{partition_start}.txt")}), "2026-01-02")
    assert "window file" in prompt_of(fake_litellm)
