"""Fixes from the official-documentation audit of the SQL-mode warehouse code (nobody can run the warehouses; the docs are the check).

Executed here through the DuckDB fakes: the Snowflake classify column holds the bare label (the stub returns an OBJECT like the real
CLASSIFY_TEXT), SAFE.COSINE_DISTANCE and the Databricks version hash. Only emitted/parsed (no engine here): label limits, INFORMATION_SCHEMA path
forms, backticked BigQuery table paths."""
import sqlglot
import pytest

from . import ai_stubs
from .bq_fake import FakeBigQueryClient
from .conftest import load_component_module
from .sql_fake import FakeWarehouse
from .test_sql_incremental import DOCS, LABELS, h  # noqa: F401  (h is a fixture)
from .test_sql_mode import STEPS_FULL, build


@pytest.fixture()
def mod():
    return load_component_module()


# ── S1: CLASSIFY_TEXT returns an OBJECT {"label": ...} ───────────────────────────────────────────────

def test_snowflake_classify_takes_the_label_field_of_the_object(mod):
    expr = mod._sql_classify_expr("snowflake_cortex", "t", ["a", "b"])
    assert expr.endswith("):label::STRING") and expr.startswith("SNOWFLAKE.CORTEX.CLASSIFY_TEXT(t,")
    assert ":label" not in mod._sql_classify_expr("bigquery", "t", ["a", "b"]) + mod._sql_classify_expr("databricks", "t", ["a", "b"])


def test_the_snowflake_stub_is_object_shaped_like_the_real_function():
    wh = FakeWarehouse("snowflake")
    try:
        raw = wh.con.execute("SELECT cortex_classify_text('x', ['a', 'b'])").fetchone()[0]
        assert raw.startswith("{") and '"label"' in raw                         # an object, not the bare label
        got = wh.rows("SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT('x', ARRAY_CONSTRUCT('a', 'b')):label::STRING AS c")[0]["c"]
        assert got in ("a", "b")
    finally:
        wh.close()


def test_snowflake_stores_the_bare_label_on_full_and_incremental_builds(h):  # noqa: F811
    if h.dialect != "snowflake_cortex":
        pytest.skip("Snowflake-only: the other dialects' classify functions return the label itself")
    h.run()                                                        # first run: the full (non-incremental) statement
    assert h.table_rows() and all(r["category"] in LABELS for r in h.table_rows())
    h.set_docs({**DOCS, "d1": "alpha beta CHANGED"})
    h.run()                                                        # second run: the incremental statement classifies only the new chunk
    assert [t for t, _ in h.ai.classify] == ["alpha beta CHANGED"]
    rows = h.table_rows()
    assert all(r["category"] in LABELS for r in rows), [r["category"] for r in rows]   # no '{"label": ..}' object text anywhere


# ── S2: classify label limits per dialect ────────────────────────────────────────────────────────────

def _steps(labels):
    return [STEPS_FULL[0], {"id": "k", "op": "classify", "candidate_labels": labels}]


@pytest.mark.parametrize("dialect,labels,msg", [
    ("snowflake_cortex", ["only"], "2 to 100"),
    ("snowflake_cortex", [f"l{i}" for i in range(101)], "2 to 100"),
    ("snowflake_cortex", ["a", "a"], "unique"),
    ("databricks", ["only"], "2 to 20"),
    ("databricks", [f"l{i}" for i in range(21)], "2 to 20"),
    ("databricks", ["a", ""], "1 to 50 characters"),
    ("databricks", ["a", "x" * 51], "1 to 50 characters"),
])
def test_out_of_range_labels_fail_at_build_time_with_a_clear_message(mod, dialect, labels, msg):
    with pytest.raises(ValueError, match=msg):
        build(mod, dialect, steps=_steps(labels))


@pytest.mark.parametrize("dialect,labels", [
    ("snowflake_cortex", ["a", "b"]), ("snowflake_cortex", [f"l{i}" for i in range(100)]),
    ("databricks", ["a", "b"]), ("databricks", [f"l{i}" for i in range(20)]), ("databricks", ["x" * 50, "y"]),
    ("bigquery", ["only"]), ("bigquery", [f"l{i}" for i in range(150)]),      # BigQuery documents no limit
])
def test_labels_inside_the_documented_limits_build(mod, dialect, labels):
    assert "CLASSIFY" in build(mod, dialect, steps=_steps(labels)).upper()


# ── B3: INFORMATION_SCHEMA path forms ────────────────────────────────────────────────────────────────

class _Rec:
    def __init__(self):
        self.sql = []

    def fetch(self, sql):
        self.sql.append(sql)
        return []


def test_bigquery_information_schema_uses_the_documented_forms(mod):
    s = _Rec()
    mod._sql_table_columns(s, "bigquery", "my-proj.ds.kb")
    mod._sql_table_columns(s, "bigquery", "`my-proj.ds.kb`")
    mod._sql_table_columns(s, "bigquery", "ds.kb")
    assert s.sql[0].split(" FROM ")[1].startswith("`my-proj`.ds.INFORMATION_SCHEMA.COLUMNS ")
    assert s.sql[1].split(" FROM ")[1].startswith("`my-proj`.ds.INFORMATION_SCHEMA.COLUMNS ")
    assert s.sql[2].split(" FROM ")[1].startswith("ds.INFORMATION_SCHEMA.COLUMNS ")
    for q in s.sql:
        sqlglot.parse_one(q, read="bigquery")


# ── B4: one backticked path everywhere for BigQuery ──────────────────────────────────────────────────

def test_bigquery_output_and_stage_are_one_backticked_path(mod):
    out = mod._sql_table("my-proj.ds.kb", "output_table", "bigquery")
    assert out == "`my-proj.ds.kb`" == mod._sql_table("`my-proj`.`ds`.`kb`", "output_table", "bigquery")
    stage = mod._sql_stage_table(out, "2026-01-01", "bigquery")
    assert stage.startswith("`my-proj.ds.kb__stage_") and stage.endswith("`") and stage.count("`") == 2
    steps = [STEPS_FULL[0], STEPS_FULL[1]]
    src = {"sql": "SELECT id, body FROM ds.src"}
    create = mod._build_sql_mode_query(steps, src, "bigquery", "my-proj.ds.kb", "id", "body", [], asset_name="kb", code_version="v1",
                                       partition="p", incremental=True, reuse={"embed": False, "classify": True}, write="stage", stage_table=stage)
    assert f"CREATE OR REPLACE TABLE {stage}" in create and "FROM `my-proj.ds.kb`" in create
    swap = " ".join(mod._sql_swap_partition_script("bigquery", out, stage, ["id", "source_partition"], "p", []))
    assert f"DELETE FROM {out}" in swap and f"INSERT INTO {out}" in swap and f"FROM {stage}" in swap
    for stmt in (create, swap):
        assert not [t for t in sqlglot.parse(stmt, read="bigquery") for tb in t.find_all(sqlglot.exp.Table) if "-" in tb.name]
    # no UNQUOTED dashed project id anywhere in the DDL / DML
    import re
    assert not re.search(r"(?<!`)\bmy-proj\b(?!\.)", create + swap) and "my-proj.ds" not in re.sub(r"`[^`]*`", "", create + swap)
    assert mod._sql_stage_table("ds.kb", "p", "snowflake_cortex") == mod._sql_stage_table("ds.kb", "p")        # other dialects unchanged


@pytest.mark.parametrize("bad", ["proj.my-ds.kb", "proj.ds.my-kb", "my-ds.kb", "a.b.c.d", "p.d.t t"])
def test_bigquery_dashes_are_only_allowed_in_the_project(mod, bad):
    with pytest.raises(ValueError):
        mod._sql_table(bad, "output_table", "bigquery")
    assert mod._sql_table("my-proj.ds.kb_2", "output_table", "bigquery") == "`my-proj.ds.kb_2`"


# ── B5: SAFE.COSINE_DISTANCE ─────────────────────────────────────────────────────────────────────────

def test_bigquery_cosine_is_safe_and_executes_to_null_on_a_dimension_mismatch(mod):
    expr = mod._sql_cosine_similarity("bigquery", "c.embedding", "q.qemb")
    assert "SAFE.COSINE_DISTANCE(c.embedding, q.qemb)" in expr
    sqlglot.parse_one(f"SELECT {expr} AS s FROM t c, u q", read="bigquery")
    client = FakeBigQueryClient("proj")
    try:
        ok = client.query("SELECT 1 - SAFE.COSINE_DISTANCE([1.0, 2.0], [1.0, 2.0]) AS s").result()
        assert abs(list(ok)[0]["s"] - 1.0) < 1e-9          # identical vectors: distance 0, similarity 1
        assert list(client.query("SELECT 1 - SAFE.COSINE_DISTANCE([1.0, 2.0], [1.0, 2.0, 3.0]) AS s").result())[0]["s"] is None
        with pytest.raises(Exception):                                                  # the unsafe form errors, as BigQuery's does
            client.query("SELECT 1 - COSINE_DISTANCE([1.0, 2.0], [1.0, 2.0, 3.0]) AS s").result()
    finally:
        client.close()


# ── D2 ───────────────────────────────────────────────────────────────────────────────────────────────

def test_databricks_from_json_uses_the_documented_schema_form_and_version_does_not_sort_structs(mod):
    sql = build(mod, "databricks", receipts=mod._validate_receipts({"dataset": "cat.rcpt"}, [], "sql", "databricks", {"resource_key": "wh"}))
    assert "from_json(ids, 'ARRAY<STRING>')" in sql and "array<string>" not in sql
    agg = mod._sql_version_agg("databricks")
    assert "struct" not in agg.lower() and "sort_array(collect_list(concat(" in agg


def test_databricks_version_hash_is_chunk_id_ordered_and_equals_the_other_dialects(mod):
    wh = FakeWarehouse("databricks")
    try:
        wh.con.execute("CREATE TABLE t(chunk_id VARCHAR, content_hash VARCHAR)")
        import hashlib
        rows = [(f"kb:d_{i}", hashlib.sha256(str(i).encode()).hexdigest()) for i in (10, 2, 1, 0, 11, 3)]   # d_1 < d_10 in plain string order
        for r in rows:
            wh.con.execute("INSERT INTO t VALUES (?, ?)", list(r))
        got = wh.rows(f"SELECT {mod._sql_version_agg('databricks')} AS v FROM t")[0]["v"]
        assert got == hashlib.md5("".join(h_ for _, h_ in sorted(rows)).encode()).hexdigest()
    finally:
        wh.close()
