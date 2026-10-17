"""execution_mode='sql': dialect-native SQL, provenance stamping, and receipts feedback computed in the warehouse.

What is executed vs only parsed:
- every dialect's statement is PARSED with sqlglot in its own dialect (snowflake_cortex -> snowflake);
- BigQuery chunk + stamp + receipts statements are EXECUTED here (Snowflake / Databricks: test_sql_mode_dialects.py) (parsed as BigQuery, transpiled, run in DuckDB via bq_fake) against real tables;
- the warehouse AI functions (ML.GENERATE_EMBEDDING, AI.CLASSIFY, Cortex, ai_query) cannot run anywhere here: those statements are parse-only.
Feedback correctness is asserted against the real ContextReceiptStore.item_feedback / outcomes_by_code_version on the SAME events."""
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
import sqlglot

from .bq_fake import FakeBigQueryClient, FakeBigQueryResource
from .conftest import load_component_module

STORE_PATH = Path(__file__).resolve().parents[4] / "resources" / "context_receipt_store" / "component.py"
DIALECT_OF = {"bigquery": "bigquery", "databricks": "databricks", "snowflake_cortex": "snowflake"}
SRC = {"sql": "SELECT id, body, cust FROM ds.src"}
STEPS_FULL = [
    {"id": "c", "op": "chunk", "chunk_size": 100, "chunk_overlap": 10, "chunk_id_prefix": "kb:"},
    {"id": "k", "op": "classify", "candidate_labels": ["billing", "it's", "back\\slash"]},
    {"id": "e", "op": "embed", "model": "p.d.m"},
]


@pytest.fixture()
def mod():
    return load_component_module()


def _rcfg(mod, **kw):
    return mod._validate_receipts({"dataset": "proj.rcpt", **kw}, [], "sql", "bigquery", {"resource_key": "bq"})


def build(mod, dialect, steps=STEPS_FULL, receipts=None, stamp=True, metas=("cust",), **kw):
    return mod._build_sql_mode_query(steps, SRC, dialect, "ds.kb", "id", "body", list(metas), asset_name="kb", code_version="v1",
                                     partition="2026-01-01", stamp_provenance=stamp, receipts=receipts, **kw)


# ── parse validity per dialect ───────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect", list(DIALECT_OF))
@pytest.mark.parametrize("stamp", [True, False])
def test_every_dialect_emits_sql_that_parses_in_that_dialect(mod, dialect, stamp):
    sql = build(mod, dialect, stamp=stamp)
    sqlglot.parse_one(sql, read=DIALECT_OF[dialect])
    assert ("source_version" in sql) is stamp


def test_bigquery_with_receipts_classify_embed_parses(mod):
    sql = build(mod, "bigquery", receipts=_rcfg(mod, quarantine={"mode": "flag"}))
    sqlglot.parse_one(sql, read="bigquery")
    assert "feedback_quarantined" in sql


def test_bigquery_exclude_mode_parses(mod):
    sqlglot.parse_one(build(mod, "bigquery", receipts=_rcfg(mod, quarantine={"mode": "exclude"})), read="bigquery")


def test_snowflake_form_is_not_emitted_for_other_dialects(mod):
    for d in ("bigquery", "databricks"):
        sql = build(mod, d)
        assert "::string" not in sql and "FLATTEN" not in sql
    assert "LATERAL FLATTEN" in build(mod, "snowflake_cortex") and "chunk.value::string" in build(mod, "snowflake_cortex")


def test_dialect_native_shapes(mod):
    bq, dbx = build(mod, "bigquery"), build(mod, "databricks")
    assert "WITH OFFSET AS chunk_index" in bq and "GENERATE_ARRAY(1, LENGTH(src.body), 90)" in bq
    assert "ML.GENERATE_EMBEDDING(MODEL `p.d.m`" in bq and "flatten_json_output" in bq and "ml_generate_embedding_result AS embedding" in bq
    assert "AI.CLASSIFY" in bq and "TO_HEX(SHA256(" in bq and "CAST(chunked.id AS STRING)" in bq
    assert "posexplode(" in dbx and "greatest(length(src.body), 1)" in dbx and "ai_classify" in dbx and "ai_query('p.d.m'" in dbx
    assert "sha2(" in dbx and "collect_list" in dbx


def test_single_quotes_and_backslashes_in_labels_and_models_are_escaped(mod):
    sql = build(mod, "bigquery", steps=[STEPS_FULL[0], {"id": "k", "op": "classify", "candidate_labels": ["x'], 1) --"]}])
    tree = sqlglot.parse_one(sql, read="bigquery")
    labels = [lit.name for lit in tree.find_all(sqlglot.exp.Literal) if lit.is_string and "--" in lit.name]
    assert labels == ["x'], 1) --"]          # one literal, quote did not terminate it
    sf = build(mod, "snowflake_cortex", steps=[STEPS_FULL[0], {"id": "e", "op": "embed", "model": "m', 'x"}])
    # the model literal appears twice: the embed call and the embedding_model column; both must be one intact literal
    assert {lit.name for lit in sqlglot.parse_one(sf, read="snowflake").find_all(sqlglot.exp.Literal) if "m'" in lit.name} == {"m', 'x"}
    with pytest.raises(ValueError, match="BigQuery model"):
        build(mod, "bigquery", steps=[STEPS_FULL[0], {"id": "e", "op": "embed", "model": "m` ) --"}])


@pytest.mark.parametrize("kw,match", [
    (dict(metas=("cust; DROP TABLE x",)), "plain identifier"),
    (dict(metas=("chunk_id",)), "collides"),
])
def test_identifiers_are_validated(mod, kw, match):
    with pytest.raises(ValueError, match=match):
        build(mod, "bigquery", **kw)
    with pytest.raises(ValueError, match="valid table reference"):
        mod._build_sql_mode_query(STEPS_FULL[:1], SRC, "bigquery", "ds.kb; DROP TABLE x", "id", "body", [])
    with pytest.raises(ValueError, match="plain identifier"):
        mod._build_sql_mode_query(STEPS_FULL[:1], SRC, "bigquery", "ds.kb", "id) --", "body", [])


def test_receipts_validation(mod):
    base = [{"id": "c", "op": "chunk"}]
    sql_comp = lambda **r: mod._validate_receipts(r, base, "sql", r.pop("_d", "bigquery"), {"resource_key": "bq"})  # noqa: E731
    out = sql_comp(dataset="proj.rcpt")
    assert out["resource_key"] == "bq" and out["table_prefix"] == "" and out["dataset"] == "proj.rcpt"
    with pytest.raises(ValueError, match="bigquery / snowflake_cortex / databricks only"):
        mod._validate_receipts({"dataset": "d"}, base, "sql", "postgres", {"resource_key": "r"})
    with pytest.raises(ValueError, match="bigquery / snowflake_cortex / databricks only"):
        mod._validate_receipts({"dataset": "d"}, base, "sql", None, {"resource_key": "r"})
    assert mod._validate_receipts({"dataset": "DB.SCH"}, base, "sql", "snowflake_cortex", {"resource_key": "r"})["sql_dialect"] == "snowflake_cortex"
    with pytest.raises(ValueError, match="dataset is required"):
        sql_comp(window_days=3)
    with pytest.raises(ValueError, match="dataset is required"):
        sql_comp(dataset="a.b.c`; drop")
    with pytest.raises(ValueError, match="same|must be the BigQuery resource"):
        sql_comp(dataset="d", resource_key="other")
    with pytest.raises(ValueError, match="table_prefix"):
        sql_comp(dataset="d", table_prefix="x-y")
    with pytest.raises(ValueError, match="sql' only"):
        mod._validate_receipts({"resource_key": "r", "dataset": "d"}, [{"id": "w", "op": "write_vector_store"}], "python")
    # no write_vector_store needed in sql mode
    assert sql_comp(dataset="d", quarantine={"mode": "exclude"})["quarantine"]["mode"] == "exclude"


# ── executed in DuckDB through the BigQuery fake: chunk + stamp ──────────────────────────────────────────

LONG = " ".join(f"w{i:02d}" for i in range(40))  # 159 chars


def make_client():
    c = FakeBigQueryClient("proj")
    c.con.execute("CREATE SCHEMA ds")
    c.con.execute("CREATE SCHEMA rcpt")
    c.con.execute(f"CREATE TABLE ds.src AS SELECT 'd1' AS id, '{LONG}' AS body, 'x' AS cust UNION ALL SELECT 'd2', 'short one', 'y' "
                  f"UNION ALL SELECT 'blank', '   ', 'z' UNION ALL SELECT 'nul', NULL, 'z'")
    return c


def rows(c, sql):
    return [dict(r) for r in c.query(sql).result()]


CHUNK_ONLY = [{"id": "c", "op": "chunk", "chunk_size": 50, "chunk_overlap": 10, "chunk_id_prefix": "kb:"}]


def test_bigquery_chunks_ids_hashes_and_stamps_execute(mod):
    import hashlib
    c = make_client()
    c.query(build(mod, "bigquery", steps=CHUNK_ONLY))
    got = rows(c, "SELECT * FROM ds.kb ORDER BY chunk_id")
    ids = [r["chunk_id"] for r in got]
    step = 40
    expected_chunks = [LONG[i:i + 50] for i in range(0, len(LONG), step) if LONG[i:i + 50].strip()]
    d1 = sorted((r for r in got if r["id"] == "d1"), key=lambda r: r["chunk_index"])
    assert [r["chunk_text"] for r in d1] == expected_chunks                 # fixed stride, 0-based offsets, in order
    assert [r["chunk_index"] for r in d1] == list(range(len(expected_chunks)))
    assert {r["id"] for r in got} == {"d1", "d2"}                           # blank and NULL text produce no chunks
    assert "kb:d1_0" in ids and "kb:d2_0" in ids
    for r in got:
        assert r["content_hash"] == hashlib.sha256(r["chunk_text"].encode()).hexdigest()
        assert r["chunk_id"] == f"kb:{r['id']}_{r['chunk_index']}"
        assert (r["source_asset_key"], r["source_partition"], r["source_code_version"], r["cust"] is not None) == ("kb", "2026-01-01", "v1", True)
    versions = {r["source_version"] for r in got}
    assert len(versions) == 1
    # source_version is the content version, and reading it back from the table gives the same value
    expected = hashlib.md5("".join(r["content_hash"] for r in sorted(got, key=lambda r: r["chunk_id"])).encode()).hexdigest()
    assert versions == {expected}
    assert rows(c, f"SELECT {mod._sql_version_agg('bigquery')} AS v FROM ds.kb")[0]["v"] == expected


def test_stamp_provenance_false_omits_source_columns_but_keeps_ids(mod):
    c = make_client()
    c.query(build(mod, "bigquery", steps=CHUNK_ONLY, stamp=False))
    cols = set(rows(c, "SELECT * FROM ds.kb")[0])
    assert {"chunk_id", "content_hash", "chunk_text", "chunk_index", "id", "cust", "source_partition"} == cols   # a partitioned run always stamps its partition
    c2 = make_client()
    c2.query(mod._build_sql_mode_query(CHUNK_ONLY, SRC, "bigquery", "ds.kb", "id", "body", ["cust"], asset_name="kb", code_version="v1", stamp_provenance=False))
    assert {"chunk_id", "content_hash", "chunk_text", "chunk_index", "id", "cust"} == set(rows(c2, "SELECT * FROM ds.kb")[0])   # unpartitioned: none


def test_content_version_changes_with_content_only(mod):
    c = make_client()
    sql = build(mod, "bigquery", steps=CHUNK_ONLY)
    c.query(sql)
    v1 = rows(c, "SELECT DISTINCT source_version AS v FROM ds.kb")[0]["v"]
    c.query(sql)
    assert rows(c, "SELECT DISTINCT source_version AS v FROM ds.kb")[0]["v"] == v1
    c.con.execute("UPDATE ds.src SET body = body || ' changed text here' WHERE id = 'd2'")
    c.query(sql)
    assert rows(c, "SELECT DISTINCT source_version AS v FROM ds.kb")[0]["v"] != v1


# ── receipts: warehouse feedback == ContextReceiptStore.item_feedback on the same events ───────────────

def load_store(path):
    spec = importlib.util.spec_from_file_location("receipt_store_for_sql_tests", STORE_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.ContextReceiptStore(str(path))


def lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


TS_COLS = {"created_at", "occurred_at", "updated_at"}


def dump_store_to_warehouse(mod, store, client, dataset="proj.rcpt"):
    """Copy every row of the sqlite store into the BigQuery-shaped tables (DDL from the canonical block, rows by SQL INSERT)."""
    for ddl in mod.receipt_table_ddl(dataset):
        client.query(ddl)
    with store._connect() as conn:
        for name in mod.RECEIPT_TABLE_NAMES:
            cur = conn.execute(f"SELECT * FROM {name} ORDER BY rowid" if name != "context_item_scores" else f"SELECT * FROM {name}")
            for row in cur.fetchall():
                d = dict(row)
                vals = [("TIMESTAMP " + lit(v)) if k in TS_COLS else lit(v) for k, v in d.items()]
                client.query(f"INSERT INTO {mod.receipt_table(dataset, name)} ({', '.join(d)}) VALUES ({', '.join(vals)})")


def item(i, cv="v1", asset="kb"):
    return {"item_id": i, "source_asset_key": asset, "source_code_version": cv, "source_version": "x"}


def build_scenario_store(tmp_path):
    """Real events in a real SQLite ContextReceiptStore (shared by the BigQuery / Snowflake / Databricks tests)."""
    store = load_store(tmp_path / "r.sqlite3")
    ids = ["kb:d1_0", "kb:d1_1", "kb:d1_2", "kb:d2_0"]
    rec = lambda items, rid=None: store.record_retrieval(context_name="kb", items=items, receipt_id=rid)  # noqa: E731
    r1 = rec([item(ids[0]), item(ids[1])], "r1")
    store.record_outcome(r1, signal="x", value=1.0, outcome_ref="u1")
    store.record_outcome(r1, signal="x", value=-1.0, outcome_ref="u1")      # u1 changes its mind: supersedes
    store.record_outcome(r1, signal="x", value=0.5, outcome_ref="u2")       # effective mean of r1 = (-1 + 0.5) / 2
    r2 = rec([item(ids[0])], "r2")
    store.record_outcome(r2, signal="x", value=-0.5)                        # anonymous outcomes never supersede
    store.record_outcome(r2, signal="x", value=-0.7)
    r3 = rec([item(ids[0]), item(ids[3])], "r3")                            # no outcomes
    store.add_evidence(r3, level="cited", details={})                       # unscoped: counts for every item of r3
    r4 = rec([item(ids[0]), item(ids[3]), item(ids[2])], "r4")
    store.record_outcome(r4, signal="x", value=-1.0)
    store.add_evidence(r4, level="verified", details={"item_ids": [ids[3]]})  # scoped: only d2_0
    store.add_evidence(r4, level="cited", details={"item_ids": []})  # scoped to nothing
    r5 = rec([item(ids[0], cv="v0")], "r5")                                  # earlier recipe
    store.record_outcome(r5, signal="x", value=1.0)
    r6 = rec([item(ids[0], cv=None)], "r6")                                  # no code version recorded => 'unknown'
    store.record_outcome(r6, signal="x", value=1.0)
    r7 = rec([item(ids[0], asset="other")], "r7")                            # another asset
    store.record_outcome(r7, signal="x", value=-1.0)
    r8 = rec([item(ids[0])], "r8")                                           # older than the window
    store.record_outcome(r8, signal="x", value=-1.0)
    old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE context_receipts SET created_at = ? WHERE receipt_id = 'r8'", (old,))
    store.refresh_item_scores(prior_strength=5.0)
    return store, ids


@pytest.fixture()
def scenario(mod, tmp_path):
    """The store scenario, mirrored into the BigQuery-shaped warehouse tables."""
    store, ids = build_scenario_store(tmp_path)
    client = make_client()
    dump_store_to_warehouse(mod, store, client)
    return SimpleNamespace(store=store, client=client, ids=ids)


FB = ["feedback_retrievals", "feedback_rated", "feedback_mean_outcome", "feedback_cited", "feedback_prior"]


def expected_for(store, cid, recipe, since):
    fb = store.item_feedback("kb", since=since, code_version=recipe).get(cid, {})
    prior = (store.get_item_scores([cid]).get(cid) or {}).get("prior") or 0.0
    return (fb.get("retrievals", 0), fb.get("rated", 0), float(fb.get("mean_outcome") or 0.0), fb.get("cited", 0), prior)


def _seed_matching_source(client):
    """A source whose chunk ids are exactly kb:d1_0, kb:d1_1, kb:d1_2, kb:d2_0 (+ a never-retrieved one)."""
    client.con.execute("DROP TABLE ds.src")
    client.con.execute("CREATE TABLE ds.src AS SELECT 'd1' AS id, 'aaaa bbbb cccc' AS body UNION ALL SELECT 'd2', 'zzzz' UNION ALL SELECT 'd3', 'never retrieved'")


FEEDBACK_STEPS = [{"id": "c", "op": "chunk", "chunk_size": 4, "chunk_overlap": 0, "chunk_id_prefix": "kb:"}]


def feedback_rows(mod, sc, **rkw):
    _seed_matching_source(sc.client)
    rc = _rcfg(mod, **rkw)
    since = mod._receipts_since(rc)
    sql = mod._build_sql_mode_query(FEEDBACK_STEPS, {"sql": "SELECT id, body FROM ds.src"}, "bigquery", "ds.kb", "id", "body", [],
                                    asset_name="kb", code_version="v1", stamp_provenance=True, receipts=rc, since=since)
    sc.client.query(sql)
    return rows(sc.client, "SELECT * FROM ds.kb ORDER BY chunk_id"), rc, since, sql


@pytest.mark.parametrize("scope", [True, False])
def test_feedback_columns_equal_store_item_feedback(mod, scenario, scope):
    got, rc, since, _ = feedback_rows(mod, scenario, scope_to_recipe=scope)
    assert {"kb:d1_0", "kb:d1_1", "kb:d1_2", "kb:d2_0", "kb:d3_0"} <= {r["chunk_id"] for r in got}
    recipe = "v1" if scope else None
    nontrivial = 0
    for r in got:
        exp = expected_for(scenario.store, r["chunk_id"], recipe, since)
        actual = tuple(r[c] for c in FB)
        assert actual == pytest.approx(exp), r["chunk_id"]
        nontrivial += exp[0] > 0
    assert nontrivial >= 3
    by = {r["chunk_id"]: r for r in got}
    assert by["kb:d3_0"]["feedback_retrievals"] == 0 and by["kb:d3_0"]["feedback_prior"] == 0.0
    # the interesting semantics, pinned to concrete numbers so a wrong-but-consistent store/warehouse pair would still fail
    d10 = by["kb:d1_0"]
    assert d10["feedback_cited"] == 1                       # r3 (unscoped cited); r4 evidence is scoped to d2_0 / to nothing => not d1_0
    assert by["kb:d2_0"]["feedback_cited"] == 2             # r3 unscoped, r4 scoped to d2_0
    assert d10["feedback_rated"] == (3 if scope else 5)     # r1 r2 r4 (+r5 r6 when not scoped to the recipe); other asset / old window never count
    assert by["kb:d1_1"]["feedback_mean_outcome"] == pytest.approx(-0.25)   # r1: u1 superseded (-1) and u2 (0.5) -> mean -0.25


def test_filter_mutations_are_caught(mod, scenario):
    """If the recipe or window filter were dropped the warehouse would disagree with the store: prove the comparison can fail."""
    got, rc, since, sql = feedback_rows(mod, scenario, scope_to_recipe=True)
    base = {r["chunk_id"]: tuple(r[c] for c in FB) for r in got}
    for old, new in [(" AND COALESCE(i.source_code_version, 'unknown') = 'v1'", ""), ("r.created_at >= ", "r.created_at >= TIMESTAMP '2000-01-01 00:00:00+00:00' OR 1=0 OR r.created_at >= ")]:
        assert old in sql
        scenario.client.query(sql.replace(old, new))
        mutated = {r["chunk_id"]: tuple(r[c] for c in FB) for r in rows(scenario.client, "SELECT * FROM ds.kb")}
        mismatch = [k for k in base if mutated[k] != pytest.approx(base[k])]
        assert mismatch, f"mutation {new!r} went unnoticed"


def test_quarantine_flag_and_exclude(mod, scenario):
    kw = dict(min_retrievals=2, min_outcomes=2, quarantine={"mean_outcome_below": -0.2, "mode": "flag"})
    got, rc, since, _ = feedback_rows(mod, scenario, **kw)
    exp = {r["chunk_id"]: (lambda e: e[0] >= 2 and e[1] >= 2 and e[2] <= -0.2)(expected_for(scenario.store, r["chunk_id"], "v1", since)) for r in got}
    assert {r["chunk_id"]: r["feedback_quarantined"] for r in got} == exp
    assert any(exp.values()) and not all(exp.values())
    flagged = {k for k, v in exp.items() if v}
    n_flag = len(got)

    got2, *_ = feedback_rows(mod, scenario, **{**kw, "quarantine": {"mean_outcome_below": -0.2, "mode": "exclude"}})
    assert {r["chunk_id"] for r in got2} == {r["chunk_id"] for r in got} - flagged and len(got2) == n_flag - len(flagged)
    assert "feedback_quarantined" not in got2[0]
    # the version stamped on the surviving rows is the version of the survivors
    assert {r["source_version"] for r in got2} == {rows(scenario.client, f"SELECT {mod._sql_version_agg('bigquery')} AS v FROM ds.kb")[0]["v"]}
    assert {r["source_version"] for r in got2} != {r["source_version"] for r in got}

    got3, *_ = feedback_rows(mod, scenario, stamp=False, **kw)           # stamp:false => no feedback_* columns, quarantine still flags
    assert set(got3[0]) & set(FB) == set() and "feedback_quarantined" in got3[0]


def test_table_prefix_is_honoured(mod, scenario):
    sql = build(mod, "bigquery", receipts=_rcfg(mod, table_prefix="pfx_"))
    assert "`proj.rcpt.pfx_context_receipt_items`" in sql and "`proj.rcpt.pfx_context_item_scores`" in sql


# ── recipe_outcomes check parity ─────────────────────────────────────────────────────────────────────────

def test_outcomes_by_code_version_sql_equals_store(mod, scenario):
    rc = _rcfg(mod)
    since = mod._receipts_since(rc)
    got = {r["code_version"]: r for r in rows(scenario.client, mod._sql_outcomes_by_code_version_query(rc, "kb", since))}
    exp = scenario.store.outcomes_by_code_version("kb", since)
    assert set(got) == set(exp) == {"v0", "v1", "unknown"}
    for cv, e in exp.items():
        assert (got[cv]["receipts"], got[cv]["outcomes"]) == (e["receipts"], e["outcomes"])
        assert got[cv]["mean_outcome"] == pytest.approx(e["mean_outcome"])


@pytest.mark.parametrize("cur_version,max_drop", [("v1", 0.15), ("v1", 5.0), ("v0", 0.15), ("never-built", 0.15)])
def test_recipe_outcomes_check_decision_is_shared_across_modes(mod, scenario, cur_version, max_drop):
    cfg = {**_rcfg(mod), "regression_check": {"min_outcomes": 1, "max_drop": max_drop, "severity": "ERROR"}}
    py_ctx = SimpleNamespace(resources=SimpleNamespace(bq=scenario.store))
    py = mod._run_recipe_outcomes_check(cfg, "kb", cur_version, py_ctx)
    sql_ctx = SimpleNamespace(resources=SimpleNamespace(bq=FakeBigQueryResource(scenario.client)))
    by_cv = mod._sql_fetch_outcomes_by_code_version(sql_ctx, {"resource_key": "bq"}, cfg, "kb")
    sq = mod._run_recipe_outcomes_check(cfg, "kb", cur_version, sql_ctx, outcomes_by_cv=by_cv)
    assert (sq.passed, sq.description) == (py.passed, py.description)
    assert {k: getattr(v, "value", v) for k, v in sq.metadata.items() if k != "outcomes_by_code_version"} == \
           {k: getattr(v, "value", v) for k, v in py.metadata.items() if k != "outcomes_by_code_version"}


def test_regression_check_can_fail_in_sql_mode(mod, scenario):
    cfg = {**_rcfg(mod), "regression_check": {"min_outcomes": 1, "max_drop": 0.01, "severity": "WARN"}}
    ctx = SimpleNamespace(resources=SimpleNamespace(bq=FakeBigQueryResource(scenario.client)))
    res = mod._run_recipe_outcomes_check(cfg, "kb", "v1", ctx, outcomes_by_cv=mod._sql_fetch_outcomes_by_code_version(ctx, {}, cfg, "kb"))
    assert res.passed is False and res.metadata["status"].value == "regressed"   # v0 (+1.0) beat v1


# ── real Dagster materialization through a fake BigQuery resource ────────────────────────────────────────

def component(mod, **kw):
    base = dict(asset_name="kb", source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body, cust FROM ds.src"},
                id_column="id", text_column="body", metadata_columns=["cust"], execution_mode="sql", sql_dialect="bigquery",
                output_table="ds.kb", code_version="v1", steps=CHUNK_ONLY)
    base.update(kw)
    return mod.ContextEngineeringPipelineComponent(**base)


def test_materialize_sql_asset_end_to_end_with_receipts_and_check(mod, scenario):
    _seed_matching_source(scenario.client)
    comp = component(mod, steps=FEEDBACK_STEPS, metadata_columns=[], source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body FROM ds.src"},
                     receipts={"dataset": "proj.rcpt", "min_retrievals": 2, "min_outcomes": 2, "quarantine": {"mean_outcome_below": -0.2},
                               "regression_check": {"min_outcomes": 1, "max_drop": 0.01}})
    defs = comp.build_defs(None)
    asset_def = list(defs.assets)[0]
    assert {s.name for s in asset_def.check_specs} == {"recipe_outcomes"}
    assert asset_def.required_resource_keys - {"io_manager"} == {"bq"}
    res = dg.materialize([asset_def], resources={"bq": FakeBigQueryResource(scenario.client)}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    ev = res.get_asset_materialization_events()[0].materialization
    n = rows(scenario.client, "SELECT COUNT(*) AS n FROM ds.kb")[0]["n"]
    assert ev.metadata["dagster/row_count"].value == n and n >= 5
    version = rows(scenario.client, "SELECT DISTINCT source_version AS v FROM ds.kb")[0]["v"]
    assert ev.metadata["context_version"].value == version
    assert ev.tags["dagster/data_version"] == version
    assert ev.metadata["receipts_chunks_with_feedback"].value == 4
    assert ev.metadata["receipts_quarantined"].value == sum(r["feedback_quarantined"] for r in rows(scenario.client, "SELECT * FROM ds.kb"))
    checks = res.get_asset_check_evaluations()
    assert [(c.check_name, c.passed) for c in checks] == [("recipe_outcomes", False)]


def test_materialize_without_receipts_or_data_version(mod):
    c = make_client()
    comp = component(mod, emit_data_version=False, stamp_provenance=False)
    asset_def = list(comp.build_defs(None).assets)[0]
    assert not asset_def.check_specs
    res = dg.materialize([asset_def], resources={"bq": FakeBigQueryResource(c)}, instance=dg.DagsterInstance.ephemeral())
    ev = res.get_asset_materialization_events()[0].materialization
    # without emit_data_version Dagster derives its own version (code version + inputs), not the content version
    assert ev.tags["dagster/data_version"] != ev.metadata["context_version"].value
    assert "source_version" not in rows(c, "SELECT * FROM ds.kb")[0]


def test_partition_key_lands_in_source_partition(mod):
    c = make_client()
    comp = component(mod, partition_type="static", partition_values="p1,p2")
    asset_def = list(comp.build_defs(None).assets)[0]
    res = dg.materialize([asset_def], resources={"bq": FakeBigQueryResource(c)}, partition_key="p2", instance=dg.DagsterInstance.ephemeral())
    assert res.success
    assert {r["source_partition"] for r in rows(c, "SELECT * FROM ds.kb")} == {"p2"}


def test_session_supports_engine_connection_and_bigquery_client(mod):
    import duckdb
    class ConnRes:
        def __init__(self): self.con = duckdb.connect()
        def get_connection(self):
            import contextlib
            return contextlib.nullcontext(self.con)
    r = ConnRes()
    with mod._sql_session(r, "r") as s:
        s.execute("CREATE TABLE t AS SELECT 1 AS a")
        assert s.fetch("SELECT a FROM t") == [(1,)]
    with pytest.raises(ValueError, match="get_client"):
        with mod._sql_session(object(), "r"):
            pass


def test_bigquery_example_yaml_builds_and_its_sql_parses(mod):
    import yaml
    attrs = yaml.safe_load((Path(mod.__file__).parent / "example_bigquery_sql_receipts.yaml").read_text())["attributes"]
    comp = mod.ContextEngineeringPipelineComponent(**attrs)
    asset_def = list(comp.build_defs(None).assets)[0]
    assert {s.name for s in asset_def.check_specs} == {"recipe_outcomes"}
    rc = mod._validate_receipts(attrs["receipts"], [], "sql", "bigquery", attrs["source"])
    sql = mod._build_sql_mode_query(attrs["steps"], attrs["source"], "bigquery", attrs["output_table"], "ticket_id", "body", ["customer_id"],
                                    asset_name="support_kb_bq", code_version="v1", stamp_provenance=True, receipts=rc)
    sqlglot.parse_one(sql, read="bigquery")
