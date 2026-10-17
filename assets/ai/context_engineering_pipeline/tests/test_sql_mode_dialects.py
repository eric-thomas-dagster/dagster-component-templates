"""execution_mode='sql' receipts on Snowflake (snowflake_cortex) and Databricks: same semantics as BigQuery / ContextReceiptStore.

Parse-only vs executed: every statement is parsed with sqlglot in its own dialect; the chunk + stamp + receipts-feedback + outcomes
statements are ALSO executed (sqlglot -> DuckDB through sql_fake.py, with the narrow documented transpile patches) on tables loaded from a
real ContextReceiptStore, and the results are compared with ContextReceiptStore.item_feedback / outcomes_by_code_version on the same events.
The warehouse AI functions (Cortex embed/classify, ai_query, ai_classify) and Cortex's text splitter are never run (macro stand-in for the splitter).
This is NOT Snowflake / Databricks: JSON, NULL, type-coercion corners can still differ there."""
import hashlib
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
import sqlglot
import yaml

from .conftest import load_component_module
from .sql_fake import ConnectionStyleResource, CursorStyleResource, EngineStyleResource, FakeWarehouse
from .test_sql_mode import FB, FEEDBACK_STEPS, STEPS_FULL, build_scenario_store, expected_for

DIALECTS = {"snowflake_cortex": ("snowflake", "DB.SCH", "sch"), "databricks": ("databricks", "cat.sch", "sch")}
STYLES = {"cursor": CursorStyleResource, "engine": EngineStyleResource, "connection": ConnectionStyleResource}
DEFAULT_STYLE = {"snowflake_cortex": "cursor", "databricks": "engine"}


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(params=list(DIALECTS))
def dialect(request):
    return request.param


class Scn(SimpleNamespace):
    pass


@pytest.fixture()
def sc(mod, tmp_path, dialect):
    read, dataset, schema = DIALECTS[dialect]
    store, ids = build_scenario_store(tmp_path)
    wh = FakeWarehouse(read)
    wh.con.execute(f"CREATE SCHEMA {schema}")
    for ddl in mod.receipt_table_ddl(dataset, "", read):
        wh.run(ddl)
    ts_cols = {"created_at", "occurred_at", "updated_at"}
    with store._connect() as conn:
        for name in mod.RECEIPT_TABLE_NAMES:
            cur = conn.execute(f"SELECT * FROM {name} ORDER BY rowid" if name != "context_item_scores" else f"SELECT * FROM {name}")
            for row in cur.fetchall():
                d = dict(row)
                vals = []
                for k, v in d.items():
                    if k in ts_cols:
                        v = datetime.fromisoformat(v)
                        v = v if read == "snowflake" else v.replace(tzinfo=None)   # TIMESTAMP_TZ vs Databricks' UTC-instant TIMESTAMP
                    vals.append(v)
                wh.con.execute(f"INSERT INTO {schema}.{name} ({', '.join(d)}) VALUES ({', '.join('?' * len(d))})", vals)
    wh.con.execute("CREATE SCHEMA ds")
    wh.con.execute("CREATE TABLE ds.src AS SELECT 'd1' AS id, 'aaaa bbbb cccc' AS body UNION ALL SELECT 'd2', 'zzzz' UNION ALL SELECT 'd3', 'never retrieved'")
    yield Scn(store=store, wh=wh, ids=ids, dialect=dialect, dataset=dataset)
    wh.close()


def rcfg(mod, dialect, **kw):
    return mod._validate_receipts({"dataset": DIALECTS[dialect][1], **kw}, [], "sql", dialect, {"resource_key": "wh"})


def query(mod, sc, **rkw):
    rc = rcfg(mod, sc.dialect, **rkw)
    since = mod._receipts_since(rc)
    sql = mod._build_sql_mode_query(FEEDBACK_STEPS, {"sql": "SELECT id, body FROM ds.src"}, sc.dialect, "ds.kb", "id", "body", [],
                                    asset_name="kb", code_version="v1", stamp_provenance=True, receipts=rc, since=since)
    return sql, rc, since


def feedback_rows(mod, sc, **rkw):
    sql, rc, since = query(mod, sc, **rkw)
    sc.wh.run(sql)
    return sc.wh.rows("SELECT * FROM ds.kb ORDER BY chunk_id"), rc, since, sql


# ── parse-only: every shape of the statement parses in its dialect ───────────────────────────────────────

@pytest.mark.parametrize("quarantine", [None, {"mode": "flag"}, {"mode": "exclude"}])
@pytest.mark.parametrize("stamp", [True, False])
def test_full_statement_parses_in_dialect(mod, dialect, quarantine, stamp):
    rc = rcfg(mod, dialect, stamp=stamp, **({"quarantine": quarantine} if quarantine else {}), table_prefix="p_")
    sql = mod._build_sql_mode_query(STEPS_FULL, {"sql": "SELECT id, body, cust FROM ds.src"}, dialect, "ds.kb", "id", "body", ["cust"],
                                    asset_name="kb", code_version="v1", stamp_provenance=True, receipts=rc)
    sqlglot.parse_one(sql, read=DIALECTS[dialect][0])
    assert "item_feedback" in sql and ("feedback_retrievals" in sql.rsplit("\nSELECT ", 1)[1]) is stamp   # in the output columns only when stamp


def test_dialect_specific_receipt_sql_shapes(mod):
    sf, _, _ = (query_text(mod, "snowflake_cortex"), 0, 0)
    dbx = query_text(mod, "databricks")
    assert "PARSE_JSON(payload_json):outcome_ref::STRING" in sf and "PARSE_JSON(payload_json):value::FLOAT" in sf
    assert "CAST('" in sf and "AS TIMESTAMP_TZ)" in sf and "TIMESTAMP '" not in sf          # not a session-zone NTZ literal
    assert "DB.SCH.context_receipt_events" in sf and "IS_ARRAY(ids)" in sf and "COUNT_IF(" in sf and "COUNTIF(" not in sf
    assert "get_json_object(payload_json, '$.value')" in dbx and "`cat`.`sch`.`context_receipt_events`" in dbx
    assert "from_json(ids, 'array<string>')" in dbx and "array_contains(c.scoped_ids, h.item_id)" in dbx and "COUNTIF(" not in dbx
    assert "JSON_VALUE" not in sf + dbx and "UNNEST" not in sf + dbx


def query_text(mod, dialect):
    rc = rcfg(mod, dialect)
    return mod._build_sql_mode_query(FEEDBACK_STEPS, {"sql": "SELECT id, body FROM ds.src"}, dialect, "ds.kb", "id", "body", [],
                                     asset_name="kb", code_version="v1", stamp_provenance=True, receipts=rc)


def test_outcomes_query_parses(mod, dialect):
    sqlglot.parse_one(mod._sql_outcomes_by_code_version_query(rcfg(mod, dialect), "kb", "2026-01-01T00:00:00+00:00"), read=DIALECTS[dialect][0])


def test_table_prefix_and_dataset_per_dialect(mod, dialect):
    sql = mod._build_sql_mode_query(FEEDBACK_STEPS, {"sql": "SELECT id, body FROM ds.src"}, dialect, "ds.kb", "id", "body", [],
                                    asset_name="kb", code_version="v1", receipts=rcfg(mod, dialect, table_prefix="pfx_"))
    want = "DB.SCH.pfx_context_receipt_items" if dialect == "snowflake_cortex" else "`cat`.`sch`.`pfx_context_item_scores`"
    assert want in sql


def test_validation_messages(mod):
    base = [{"id": "c", "op": "chunk"}]
    for d in ("snowflake_cortex", "databricks"):
        with pytest.raises(ValueError, match="dataset is required"):
            mod._validate_receipts({"window_days": 3}, base, "sql", d, {"resource_key": "r"})
        with pytest.raises(ValueError, match="dataset is required"):
            mod._validate_receipts({"dataset": "a.b.c"}, base, "sql", d, {"resource_key": "r"})
        with pytest.raises(ValueError, match="warehouse resource"):
            mod._validate_receipts({"dataset": "a.b", "resource_key": "other"}, base, "sql", d, {"resource_key": "r"})
    with pytest.raises(ValueError, match="DATABASE.SCHEMA"):
        mod._validate_receipts({}, base, "sql", "snowflake_cortex", {"resource_key": "r"})
    with pytest.raises(ValueError, match="catalog.schema"):
        mod._validate_receipts({}, base, "sql", "databricks", {"resource_key": "r"})


# ── executed (DuckDB via sqlglot): chunk ids / content version ───────────────────────────────────────────

def test_chunk_stamp_and_content_version_execute(mod, sc):
    rows, *_ = feedback_rows(mod, sc)
    got = {r["chunk_id"]: r for r in rows}
    assert {"kb:d1_0", "kb:d1_1", "kb:d1_2", "kb:d2_0", "kb:d3_0"} <= set(got)       # fixed-stride stand-in splitter, 0-based indexes
    assert all(r["chunk_id"] == f"kb:{r['id']}_{r['chunk_index']}" for r in rows)
    assert all(r["content_hash"] == hashlib.sha256(r["chunk_text"].encode()).hexdigest() for r in rows)
    expected = hashlib.md5("".join(r["content_hash"] for r in sorted(rows, key=lambda r: r["chunk_id"])).encode()).hexdigest()
    assert {r["source_version"] for r in rows} == {expected}
    assert sc.wh.rows(f"SELECT {mod._sql_version_agg(mod_dialect(sc))} AS v FROM ds.kb")[0]["v"] == expected


def mod_dialect(sc):
    return sc.dialect


# ── executed: feedback == ContextReceiptStore.item_feedback on the same events ───────────────────────────

@pytest.mark.parametrize("scope", [True, False])
def test_feedback_columns_equal_store_item_feedback(mod, sc, scope):
    got, rc, since, _ = feedback_rows(mod, sc, scope_to_recipe=scope)
    recipe = "v1" if scope else None
    nontrivial = 0
    for r in got:
        exp = expected_for(sc.store, r["chunk_id"], recipe, since)
        assert tuple(r[c] for c in FB) == pytest.approx(exp), r["chunk_id"]
        nontrivial += exp[0] > 0
    assert nontrivial >= 3
    by = {r["chunk_id"]: r for r in got}
    assert by["kb:d3_0"]["feedback_retrievals"] == 0 and by["kb:d3_0"]["feedback_prior"] == 0.0
    assert by["kb:d1_0"]["feedback_cited"] == 1             # r3 unscoped; r4's evidence is scoped to d2_0 / to nothing
    assert by["kb:d2_0"]["feedback_cited"] == 2             # r3 unscoped + r4 scoped to d2_0
    assert by["kb:d1_0"]["feedback_rated"] == (3 if scope else 5)
    assert by["kb:d1_1"]["feedback_mean_outcome"] == pytest.approx(-0.25)    # u1 superseded (-1), u2 0.5
    assert by["kb:d1_0"]["feedback_prior"] != 0.0       # priors are joined from context_item_scores


def test_filter_mutations_are_caught(mod, sc):
    """Dropping the recipe filter, the window filter or the evidence scoping must change the result: the comparison can fail."""
    got, rc, since, sql = feedback_rows(mod, sc, scope_to_recipe=True)
    base = {r["chunk_id"]: tuple(r[c] for c in FB) for r in got}
    with_scoping = sql
    mutations = [
        (" AND COALESCE(i.source_code_version, 'unknown') = 'v1'", ""),
        (mod._sql_ts(since, sc.dialect), mod._sql_ts("2000-01-01T00:00:00+00:00", sc.dialect)),
        ("(NOT c.is_scoped OR ", "(TRUE OR "),
    ]
    for old, new in mutations:
        assert old in with_scoping, old
        sc.wh.run(with_scoping.replace(old, new))
        mutated = {r["chunk_id"]: tuple(r[c] for c in FB) for r in sc.wh.rows("SELECT * FROM ds.kb")}
        assert [k for k in base if mutated[k] != pytest.approx(base[k])], f"mutation {old!r} went unnoticed"
    sc.wh.run(with_scoping)
    assert {r["chunk_id"]: tuple(r[c] for c in FB) for r in sc.wh.rows("SELECT * FROM ds.kb")} == base      # restored => identical


def test_quarantine_flag_and_exclude(mod, sc):
    kw = dict(min_retrievals=2, min_outcomes=2, quarantine={"mean_outcome_below": -0.2, "mode": "flag"})
    got, rc, since, _ = feedback_rows(mod, sc, **kw)
    exp = {r["chunk_id"]: (lambda e: e[0] >= 2 and e[1] >= 2 and e[2] <= -0.2)(expected_for(sc.store, r["chunk_id"], "v1", since)) for r in got}
    assert {r["chunk_id"]: r["feedback_quarantined"] for r in got} == exp
    assert any(exp.values()) and not all(exp.values())
    flagged = {k for k, v in exp.items() if v}
    got2, *_ = feedback_rows(mod, sc, **{**kw, "quarantine": {"mean_outcome_below": -0.2, "mode": "exclude"}})
    assert {r["chunk_id"] for r in got2} == {r["chunk_id"] for r in got} - flagged
    assert "feedback_quarantined" not in got2[0]
    assert {r["source_version"] for r in got2} == {sc.wh.rows(f"SELECT {mod._sql_version_agg(sc.dialect)} AS v FROM ds.kb")[0]["v"]}
    assert {r["source_version"] for r in got2} != {r["source_version"] for r in got}
    got3, *_ = feedback_rows(mod, sc, stamp=False, **kw)
    assert set(got3[0]) & set(FB) == set() and "feedback_quarantined" in got3[0]


# ── recipe_outcomes check parity, through every connection style ─────────────────────────────────────────

def test_outcomes_by_code_version_sql_equals_store(mod, sc):
    rc = rcfg(mod, sc.dialect)
    since = mod._receipts_since(rc)
    got = {r["code_version"]: r for r in sc.wh.rows(mod._sql_outcomes_by_code_version_query(rc, "kb", since))}
    exp = sc.store.outcomes_by_code_version("kb", since)
    assert set(got) == set(exp) == {"v0", "v1", "unknown"}
    for cv, e in exp.items():
        assert (got[cv]["receipts"], got[cv]["outcomes"]) == (e["receipts"], e["outcomes"])
        assert got[cv]["mean_outcome"] == pytest.approx(e["mean_outcome"])


@pytest.mark.parametrize("style", list(STYLES))
@pytest.mark.parametrize("cur_version,max_drop", [("v1", 0.15), ("v1", 5.0), ("v0", 0.15), ("never-built", 0.15)])
def test_recipe_outcomes_check_decision_is_shared_across_modes(mod, sc, style, cur_version, max_drop):
    cfg = {**rcfg(mod, sc.dialect), "regression_check": {"min_outcomes": 1, "max_drop": max_drop, "severity": "ERROR"}}
    py = mod._run_recipe_outcomes_check(cfg, "kb", cur_version, SimpleNamespace(resources=SimpleNamespace(wh=sc.store)))
    ctx = SimpleNamespace(resources=SimpleNamespace(wh=STYLES[style](sc.wh)))
    by_cv = mod._sql_fetch_outcomes_by_code_version(ctx, {"resource_key": "wh"}, cfg, "kb")
    sq = mod._run_recipe_outcomes_check(cfg, "kb", cur_version, ctx, outcomes_by_cv=by_cv)
    assert (sq.passed, sq.description) == (py.passed, py.description)
    strip = lambda m: {k: getattr(v, "value", v) for k, v in m.items() if k != "outcomes_by_code_version"}  # noqa: E731
    assert strip(sq.metadata) == strip(py.metadata)


# ── real Dagster materialization through fake resources ──────────────────────────────────────────────────

@pytest.mark.parametrize("style", list(STYLES))
def test_materialize_end_to_end_with_receipts_and_check(mod, sc, style):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", source={"kind": "warehouse_query", "resource_key": "wh", "sql": "SELECT id, body FROM ds.src"},
        id_column="id", text_column="body", metadata_columns=[], execution_mode="sql", sql_dialect=sc.dialect, output_table="ds.kb",
        code_version="v1", steps=FEEDBACK_STEPS,
        receipts={"dataset": sc.dataset, "min_retrievals": 2, "min_outcomes": 2, "quarantine": {"mean_outcome_below": -0.2},
                  "regression_check": {"min_outcomes": 1, "max_drop": 0.01}})
    asset_def = list(comp.build_defs(None).assets)[0]
    assert {s.name for s in asset_def.check_specs} == {"recipe_outcomes"}
    res = dg.materialize([asset_def], resources={"wh": STYLES[style](sc.wh)}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    ev = res.get_asset_materialization_events()[0].materialization
    rows = sc.wh.rows("SELECT * FROM ds.kb")
    assert ev.metadata["dagster/row_count"].value == len(rows) >= 5
    assert ev.metadata["context_version"].value == rows[0]["source_version"]
    assert ev.metadata["receipts_chunks_with_feedback"].value == 4
    assert ev.metadata["receipts_quarantined"].value == sum(r["feedback_quarantined"] for r in rows) > 0
    since = mod._receipts_since({"window_days": 30})     # differs from the run's own cut-off by ms only; no receipt sits near it
    for r in rows:
        assert tuple(r[c] for c in FB) == pytest.approx(expected_for(sc.store, r["chunk_id"], "v1", since)), r["chunk_id"]
    assert [(c.check_name, c.passed) for c in res.get_asset_check_evaluations()] == [("recipe_outcomes", False)]  # v0 (+1.0) beat v1


# ── example yamls ────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect_,fname", [("snowflake_cortex", "example_snowflake_sql_receipts.yaml"), ("databricks", "example_databricks_sql_receipts.yaml")])
def test_example_yaml_builds_and_its_sql_parses(mod, dialect_, fname):
    attrs = yaml.safe_load((Path(mod.__file__).parent / fname).read_text())["attributes"]
    assert attrs["sql_dialect"] == dialect_
    comp = mod.ContextEngineeringPipelineComponent(**attrs)
    asset_def = list(comp.build_defs(None).assets)[0]
    assert {s.name for s in asset_def.check_specs} == {"recipe_outcomes"}
    rc = mod._validate_receipts(attrs["receipts"], [], "sql", dialect_, attrs["source"])
    sql = mod._build_sql_mode_query(attrs["steps"], attrs["source"], dialect_, attrs["output_table"], attrs["id_column"], attrs["text_column"],
                                    attrs["metadata_columns"], asset_name=attrs["asset_name"], code_version="v1", stamp_provenance=True, receipts=rc)
    sqlglot.parse_one(sql, read=DIALECTS[dialect_][0])
