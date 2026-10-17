"""retrieval_eval in execution_mode='sql' (BigQuery / Snowflake Cortex / Databricks): the golden-set check runs warehouse-side.

What is EXECUTED: every statement, through sqlglot -> DuckDB (bq_fake.py / sql_fake.py) inside a real dg.materialize / get_implicit_global_asset_job_def run
that builds the table and then runs the check through the build's own resource; the metrics are compared with an independent numpy brute-force
(cosine over the table's stored vectors) and, when chromadb is installed, with python mode's own loop over a chromadb cosine index holding the same
vectors. STUBBED: (1) the embedding function (ML.GENERATE_EMBEDDING / EMBED_TEXT_768 / ai_query) is a deterministic bag-of-words UDF (words hashed
into 64 buckets), so cosine retrieval is meaningful -- the build and the golden queries go through the SAME stub, as they do through the same real
function; (2) the similarity function: BigQuery's COSINE_DISTANCE maps to DuckDB's list_cosine_distance (sqlglot's own mapping), Snowflake's
VECTOR_COSINE_SIMILARITY is a macro over list_cosine_similarity, Databricks' zip_with/aggregate expression has no DuckDB form so it is swapped for a macro
over list_cosine_similarity (the real Databricks expression is parse-checked in its dialect but its math is NOT executed here).
NOT run: the real engines (VECTOR column types, ML.GENERATE_EMBEDDING quotas/failures, ai_query's return type)."""
import math
import re
import zlib

import dagster as dg
import pytest
import sqlglot

from .bq_fake import FakeBigQueryResource
from .conftest import load_component_module, requires_chromadb
from .test_sql_incremental import READ, Harness
from .test_sql_mode import FEEDBACK_STEPS, _seed_matching_source, scenario  # noqa: F401  (scenario is a fixture)
from .test_sql_partitions import PHarness

DIM = 64
DOCS = {
    "billing": "invoice payment refund charge billing card",
    "shipping": "package delivery carrier tracking shipping address",
    "crashes": "app crash error stacktrace restart bug",
    "export": "export csv download report data file",
}
# (query, expected, rank of the first expected hit within k=3 -- by construction, None = miss)
GOLDEN = [
    ("how do I get a refund on a card payment", ["billing"], 1),
    ("tracking the delivery of my package", ["shipping_0"], 1),               # a chunk id
    ("export report data to csv after the app crash", ["crashes"], 2),         # export doc outranks it
    ("nothing matches this at all zzz", ["not_in_the_index"], None),
]
K = 3


def bow(text):
    v = [0.0] * DIM
    for w in re.findall(r"[a-z0-9]+", text.lower()):
        v[zlib.crc32(w.encode()) % DIM] += 1.0
    return v


def install(wh, dialect, mod):
    """Swap the call-counting embed UDFs for the bag-of-words one (still counting) and register the dialect similarity stand-in."""
    con, calls = wh.con, wh.ai

    def embed(model, text):
        calls.embed.append((model, text))
        return bow(text)

    for name in ("ai_embed", "ai_query"):
        con.remove_function(name)
        con.create_function(name, embed, ["VARCHAR", "VARCHAR"], "DOUBLE[]")
    con.execute("CREATE MACRO VECTOR_COSINE_SIMILARITY(a, b) AS list_cosine_similarity(a, b)")
    con.execute("CREATE MACRO vec_cos(a, b) AS list_cosine_similarity(a, b)")
    if dialect == "databricks":
        real = mod._sql_cosine_similarity("databricks", "c.embedding", "q.qemb")
        orig = wh.to_duckdb
        wh.to_duckdb = lambda sql: orig(sql.replace(real, "vec_cos(c.embedding, q.qemb)"))


@pytest.fixture()
def mod():
    return load_component_module()


def golden_cfg(**kw):
    return {"queries": [{"query": q, "expected": e} for q, e, _ in GOLDEN], "k": K, "min_recall": 0.5, **kw}


def py_metrics(rows, queries, k):
    """Independent hand computation of python-mode's definitions over the table's stored vectors (numpy-free brute force)."""
    def cos(a, b):
        na, nb = math.sqrt(sum(x * x for x in a)), math.sqrt(sum(x * x for x in b))
        return sum(x * y for x, y in zip(a, b)) / (na * nb) if na and nb else None
    hits, rr, misses = 0, 0.0, []
    for q in queries:
        qv = bow(q["query"])
        scored = sorted(((cos(r["embedding"], qv), r["chunk_id"], r["id"]) for r in rows if cos(r["embedding"], qv) is not None),
                        key=lambda t: (-t[0], t[1]))[:k]
        expected = {str(e) for e in q["expected"]}
        rank = next((i for i, (_, cid, sid) in enumerate(scored) if cid in expected or sid in expected), None)
        if rank is None:
            misses.append(q["query"])
        else:
            hits += 1
            rr += 1 / (rank + 1)
    return hits / len(queries), rr / len(queries), misses


@pytest.fixture(params=list(READ))
def h(request, mod):
    harness = Harness(mod, request.param)
    install(harness.wh, request.param, mod)
    harness.steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": ""},
                     {"id": "e", "op": "embed", "model": harness.model}]
    harness.set_docs(DOCS)
    yield harness
    harness.wh.close()


def check_of(res):
    evs = {e.check_name: e for e in res.get_asset_check_evaluations()}
    return evs["retrieval_quality"]


def run_with_check(h, **kw):
    h.run(retrieval_eval=golden_cfg(**kw.pop("eval", {})), **kw)
    return check_of(h.last[0])


# ── executed ────────────────────────────────────────────────────────────────────────────────────────────

def test_metrics_equal_python_mode_definitions_on_the_same_corpus(h):
    ev = run_with_check(h)
    recall, mrr, misses = py_metrics(h.table_rows(), golden_cfg()["queries"], K)
    assert (recall, mrr) == (0.75, (1 + 1 + 0.5) / 4)            # by construction of GOLDEN: ranks 1, 1, 2, miss
    assert ev.metadata["recall_at_k"].value == round(recall, 4) and ev.metadata["mrr"].value == round(mrr, 4)
    assert ev.metadata["k"].value == K and ev.metadata["queries"].value == 4
    assert ev.metadata["misses"].value == misses == [GOLDEN[3][0]]
    assert ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR


def test_thresholds_severity_and_blocking(h, mod):
    failing = run_with_check(h, eval={"min_recall": 0.8, "severity": "WARN"})
    assert not failing.passed and failing.severity == dg.AssetCheckSeverity.WARN
    assert not run_with_check(h, eval={"min_mrr": 0.7}).passed and run_with_check(h, eval={"min_mrr": 0.6}).passed
    spec = list(list(h.component(retrieval_eval=golden_cfg(blocking=True)).build_defs(None).assets)[0].check_specs)[0]
    assert spec.name == "retrieval_quality" and spec.blocking


def test_golden_queries_are_embedded_with_the_builds_model_and_no_other_model(h):
    h.run(retrieval_eval=golden_cfg())
    assert {m for m, _ in h.ai.embed} == {h.called(h.model)}
    assert sorted(t for _, t in h.ai.embed) == sorted([*DOCS.values(), *(q for q, _, _ in GOLDEN)])   # the 4 docs by the build, the 4 golden queries by the check


def test_incremental_reuse_still_evaluates_and_only_the_queries_hit_the_embedder(h):
    h.run(retrieval_eval=golden_cfg())
    before = h.table_rows()
    ev = run_with_check(h)                                                      # second run: every chunk embedding is reused
    assert sorted(t for _, t in h.ai.embed) == sorted(q for q, _, _ in GOLDEN)
    assert h.table_rows() == before and ev.metadata["recall_at_k"].value == 0.75
    h.set_docs({**DOCS, "billing": "completely different words about penguins"})
    ev = run_with_check(h)                                                      # one changed doc: 1 chunk + 4 queries embedded; metrics follow the table
    assert len(h.ai.embed) == 5 and ev.metadata["recall_at_k"].value == py_metrics(h.table_rows(), golden_cfg()["queries"], K)[0] < 0.75


def test_a_stale_model_row_is_not_compared_against_the_current_models_vectors(h):
    h.run()
    h.con.execute(f"UPDATE {h.duck} SET embedding_model = 'old-model' WHERE id = 'billing'")
    ev = run_with_check(h)                                                      # (re-run rebuilds: reuse is impossible for old-model rows, so rows are fresh)
    assert ev.metadata["recall_at_k"].value == 0.75
    sql = h.mod._build_retrieval_eval_query(golden_cfg(), h.steps, h.dialect, h.table, "id")
    assert f"embedding_model = '{h.model}'" in sql


def test_scoped_to_this_assets_rows_when_provenance_is_stamped(h):
    from types import SimpleNamespace
    h.run()
    h.con.execute(f"INSERT INTO {h.duck} SELECT * REPLACE ('other_asset' AS source_asset_key, 'decoy' AS chunk_id) FROM {h.duck} WHERE id = 'billing'")
    cfg = {"queries": [{"query": DOCS["billing"], "expected": ["decoy"]}], "k": 3, "min_recall": 0.0}
    ctx = SimpleNamespace(resources=SimpleNamespace(wh=h.resource))
    run = lambda scoped: h.mod._run_sql_retrieval_eval(  # noqa: E731
        cfg, h.steps, {"resource_key": "wh"}, h.dialect, h.table, "id", "kb", scoped, ctx).metadata["recall_at_k"].value
    assert (run(True), run(False)) == (0.0, 1.0)          # another asset's row sharing the table is invisible to this asset's check


def test_real_check_execution_through_the_implicit_global_job(h):
    defs = dg.Definitions.merge(h.component(retrieval_eval=golden_cfg()).build_defs(None), dg.Definitions(resources={"wh": h.resource}))
    res = defs.get_implicit_global_asset_job_def().execute_in_process(instance=dg.DagsterInstance.ephemeral())
    assert res.success
    ev = check_of(res)
    assert ev.metadata["recall_at_k"].value == 0.75 and ev.passed


# ── partitioned ─────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture(params=list(READ))
def p(request, mod):
    harness = PHarness(mod, request.param)
    install(harness.wh, request.param, mod)
    harness.steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0}, {"id": "e", "op": "embed", "model": harness.model}]
    harness.set_src([("billing", DOCS["billing"], "A"), ("shipping", DOCS["shipping"], "A"),
                     ("crashes", DOCS["crashes"], "B"), ("export", DOCS["export"], "B")])
    yield harness
    harness.wh.close()


def test_partitioned_run_searches_the_whole_table_not_only_its_own_partition(p):
    cfg = golden_cfg()
    p.run(partition_key="A", retrieval_eval=cfg)
    only_a = check_of(p.last[0])
    assert only_a.metadata["misses"].value == [GOLDEN[2][0], GOLDEN[3][0]]
    assert only_a.metadata["recall_at_k"].value == 0.5            # billing + shipping answered from partition A; crashes lives in B
    p.run(partition_key="B", retrieval_eval=cfg)
    both = check_of(p.last[0])                                     # B's run sees A's rows too: the index is the whole table
    assert both.metadata["recall_at_k"].value == 0.75
    assert both.metadata["mrr"].value == round(py_metrics(p.table_rows(), cfg["queries"], K)[1], 4)


# ── receipts quarantine ─────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("mode", ["flag", "exclude"])
def test_quarantine_flag_keeps_rows_searchable_exclude_removes_them(mod, scenario, mode):
    _seed_matching_source(scenario.client)
    install(scenario.client, "bigquery", mod)
    cfg = {"queries": [{"query": "aaaa", "expected": ["kb:d1_0"]}, {"query": "zzzz", "expected": ["kb:d2_0"]}], "k": 1, "min_recall": 0.0}
    steps = FEEDBACK_STEPS + [{"id": "e", "op": "embed", "model": "p.d.m1"}]
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body FROM ds.src"}, id_column="id", text_column="body",
        metadata_columns=[], execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb", code_version="v1", steps=steps, retrieval_eval=cfg,
        receipts={"dataset": "proj.rcpt", "min_retrievals": 2, "min_outcomes": 2, "quarantine": {"mean_outcome_below": -0.2, "mode": mode}})
    res = dg.materialize([list(comp.build_defs(None).assets)[0]], resources={"bq": FakeBigQueryResource(scenario.client)}, instance=dg.DagsterInstance.ephemeral())
    ev = check_of(res)
    present = {r["chunk_id"] for r in scenario.client.con.execute("SELECT chunk_id FROM ds.kb").fetch_arrow_table().to_pylist()}
    if mode == "flag":
        assert "kb:d1_0" in present and ev.metadata["recall_at_k"].value == 1.0       # flagged, still a candidate (as in python mode)
    else:
        assert "kb:d1_0" not in present and ev.metadata["recall_at_k"].value == 0.5   # excluded rows are simply not in the table


# ── parse-only + validation ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect", list(READ))
@pytest.mark.parametrize("scoped", [False, True])
def test_statement_parses_in_its_dialect(mod, dialect, scoped):
    steps = [{"id": "c", "op": "chunk"}, {"id": "e", "op": "embed", "model": "p.d.m1" if dialect == "bigquery" else None}]
    cfg = golden_cfg()
    cfg["queries"][0]["query"] = "it's a \\ tricky 'query'"
    sql = mod._build_retrieval_eval_query(cfg, steps, dialect, "ds.kb", "id", asset_name="kb", scoped=scoped)
    sqlglot.parse_one(sql, read=READ[dialect])
    assert ("ML.GENERATE_EMBEDDING" in sql) is (dialect == "bigquery")
    assert ("VECTOR_COSINE_SIMILARITY" in sql) is (dialect == "snowflake_cortex")
    assert ("COSINE_DISTANCE" in sql) is (dialect == "bigquery") and ("zip_with" in sql) is (dialect == "databricks")
    assert f"rn <= {K}" in sql and "ORDER BY sim DESC" in sql


def test_validation_is_shared_and_sql_mode_needs_only_an_embed_step(mod):
    steps = [{"id": "c", "op": "chunk"}, {"id": "e", "op": "embed"}]
    for bad, match in [({"queries": []}, "non-empty"), ({"queries": [{"query": "q"}]}, "expected"), ({"queries": [{"query": "q", "expected": ["x"]}], "k": 0}, "k"),
                       ({"queries": [{"query": "q", "expected": ["x"]}], "bogus": 1}, "unknown")]:
        with pytest.raises(ValueError, match=match):
            mod._validate_retrieval_eval(bad, steps, "sql")
    assert mod._validate_retrieval_eval(golden_cfg(), steps, "sql")
    with pytest.raises(ValueError, match="embed step"):
        mod._validate_retrieval_eval(golden_cfg(), [{"id": "c", "op": "chunk"}], "sql")
    with pytest.raises(ValueError, match="chromadb"):
        mod._validate_retrieval_eval(golden_cfg(), steps)                       # python mode still needs the chromadb index


# ── mutations ───────────────────────────────────────────────────────────────────────────────────────────

def test_mutation_wrong_k_is_caught(h, monkeypatch):
    good = run_with_check(h).metadata["recall_at_k"].value
    orig = h.mod._build_retrieval_eval_query
    monkeypatch.setattr(h.mod, "_build_retrieval_eval_query", lambda *a, **k: orig(*a, **k).replace(f"rn <= {K}", "rn <= 1"))
    assert run_with_check(h).metadata["recall_at_k"].value < good               # the rank-2 hit is lost


def test_mutation_wrong_similarity_order_is_caught(h, monkeypatch):
    good = run_with_check(h).metadata
    orig = h.mod._build_retrieval_eval_query
    monkeypatch.setattr(h.mod, "_build_retrieval_eval_query", lambda *a, **k: orig(*a, **k).replace("ORDER BY sim DESC", "ORDER BY sim ASC"))
    bad = run_with_check(h, eval={"min_recall": 0.99})
    assert not bad.passed and bad.metadata["mrr"].value < good["mrr"].value


# ── cross-check against chromadb itself ─────────────────────────────────────────────────────────────────

@requires_chromadb
def test_same_numbers_as_python_mode_loop_over_a_chromadb_index_of_the_same_vectors(h, tmp_path, mod):
    import chromadb
    ev = run_with_check(h)
    rows = h.table_rows()
    col = chromadb.PersistentClient(path=str(tmp_path / "c")).create_collection("kb_cmp", metadata={"hnsw:space": "cosine"})
    col.add(ids=[r["chunk_id"] for r in rows], embeddings=[list(r["embedding"]) for r in rows], metadatas=[{"source_id": r["id"]} for r in rows])
    ranked = []
    for q in golden_cfg()["queries"]:                                           # python mode's per-query step, verbatim
        res = col.query(query_embeddings=[bow(q["query"])], n_results=K, include=["metadatas"])
        ranked.append([(cid, (meta or {}).get("source_id")) for cid, meta in zip(res["ids"][0], res["metadatas"][0])])
    py = mod._retrieval_quality_result(golden_cfg(), ranked)
    assert {k: v.value for k, v in py.metadata.items()} == {k: v.value for k, v in ev.metadata.items()}
