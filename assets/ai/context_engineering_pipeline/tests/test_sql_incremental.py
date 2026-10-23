"""execution_mode='sql' `incremental`: only chunks with no reusable prior value reach the warehouse AI function.

Executed here (sqlglot -> DuckDB through bq_fake.py / sql_fake.py, real dg.materialize, all three dialects): the AI functions are DuckDB UDF
stand-ins (ai_stubs.py) that RECORD every call, so each test asserts exactly which rows would have been billed. The statement shapes of all three
dialects are also parse-checked in their own dialect. NOT executed anywhere here: the real ML.GENERATE_EMBEDDING / Cortex / ai_query / classify
functions, CREATE OR REPLACE TABLE reading from the table it replaces on the real engines, and the real INFORMATION_SCHEMA."""
import itertools

import dagster as dg
import pytest
import sqlglot
from sqlglot import exp

from . import ai_stubs
from .bq_fake import FakeBigQueryClient, FakeBigQueryResource
from .conftest import load_component_module
from .sql_fake import ConnectionStyleResource, CursorStyleResource, EngineStyleResource, FakeWarehouse
from .test_sql_mode import _seed_matching_source, scenario  # noqa: F401  (scenario is a fixture)

READ = {"bigquery": "bigquery", "snowflake_cortex": "snowflake", "databricks": "databricks"}
LABELS = ["billing", "technical", "other"]
LONG = " ".join(f"w{i:02d}" for i in range(30))   # 119 chars -> 3 chunks at size 50 / overlap 10
DOCS = {"d1": "alpha beta gamma", "d2": LONG, "d3": "same text", "d4": "same text"}   # d3/d4: identical chunk text
STEPS = [
    {"id": "c", "op": "chunk", "chunk_size": 50, "chunk_overlap": 10, "chunk_id_prefix": "kb:"},
    {"id": "k", "op": "classify", "candidate_labels": LABELS},
    {"id": "e", "op": "embed", "model": "MODEL_1"},
]


@pytest.fixture()
def mod():
    return load_component_module()


class Harness:
    def __init__(self, mod, dialect):
        self.mod, self.dialect = mod, dialect
        if dialect == "bigquery":
            self.wh = FakeBigQueryClient("proj")
            self.resource, self.table, self.duck, self.model = FakeBigQueryResource(self.wh), "proj.ds.kb", "ds.kb", "p.d.m1"
            self.called = lambda m: m.split(".", 1)[1]   # the fake strips the project from `p.d.m` (as it does for table names)
            self.schemas = ["ds"]
        else:
            self.wh = FakeWarehouse(READ[dialect])
            style = {"snowflake_cortex": CursorStyleResource, "databricks": EngineStyleResource}[dialect]
            self.resource, self.duck, self.model = style(self.wh), "sch.kb", "model-1"
            self.called = lambda m: m
            self.table = ("DB" if dialect == "snowflake_cortex" else "cat") + ".sch.kb"
            self.schemas = ["ds", "sch"]
        self.con, self.ai = self.wh.con, self.wh.ai
        for s in self.schemas:
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        self.set_docs(DOCS)
        self.steps = [dict(s) for s in STEPS]
        self.steps[2]["model"] = self.model
        self.last = None

    def set_docs(self, docs):
        self.con.execute("DROP TABLE IF EXISTS ds.src")
        self.con.execute("CREATE TABLE ds.src(id VARCHAR, body VARCHAR)")
        for k, v in docs.items():
            self.con.execute("INSERT INTO ds.src VALUES (?, ?)", [k, v])

    def component(self, **kw):
        base = dict(asset_name="kb", source={"kind": "warehouse_query", "resource_key": "wh", "sql": "SELECT id, body FROM ds.src"},
                    id_column="id", text_column="body", metadata_columns=[], execution_mode="sql", sql_dialect=self.dialect,
                    output_table=self.table, code_version="v1", steps=self.steps)
        base.update(kw)
        return self.mod.ContextEngineeringPipelineComponent(**base)

    def run(self, **kw):
        """Materialize through Dagster; returns the materialization metadata. self.ai holds the calls made by THIS run only."""
        self.ai.reset()
        asset_def = list(self.component(**kw).build_defs(None).assets)[0]
        instance = dg.DagsterInstance.ephemeral()
        res = dg.materialize([asset_def], resources={"wh": self.resource}, instance=instance)
        assert res.success
        self.last = (res, instance)
        return res.get_asset_materialization_events()[0].materialization.metadata

    def logs(self):
        res, instance = self.last
        return [e.user_message for e in instance.all_logs(res.run_id)]

    def table_rows(self):
        cur = self.con.execute(f"SELECT * FROM {self.duck} ORDER BY chunk_id")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def texts(self):
        return {r["chunk_text"] for r in self.table_rows()}


@pytest.fixture(params=list(READ))
def h(request, mod):
    harness = Harness(mod, request.param)
    yield harness
    harness.wh.close()


def vals(md, *names):
    return [md[n].value for n in names]


# ── executed ────────────────────────────────────────────────────────────────────────────────────────────

def test_first_run_builds_fully_and_stores_what_reuse_needs(h):
    md = h.run()
    rows = h.table_rows()
    distinct = {r["chunk_text"] for r in rows}
    assert len(rows) == 6 and len(distinct) == 5                      # d1:1, d2:3, d3:1, d4:1 (same text as d3)
    assert sorted(t for _, t in h.ai.embed) == sorted(distinct)       # identical chunk text is embedded once
    assert {m for m, _ in h.ai.embed} == {h.called(h.model)} and len(h.ai.classify) == 5
    assert vals(md, "reused_embeddings", "new_embeddings", "reused_categories", "new_categories") == [0, 6, 0, 6]
    assert {r["embedding_model"] for r in rows} == {h.model}
    assert len({r["category_signature"] for r in rows}) == 1
    assert all(len(r["embedding"]) == 3 and r["category"] in LABELS for r in rows)
    assert all(r["embedding"] == ai_stubs.vector_for(h.called(h.model), r["chunk_text"]) for r in rows)


def test_second_run_with_unchanged_corpus_makes_zero_ai_calls(h):
    h.run()
    before = h.table_rows()
    md = h.run()
    assert h.ai.embed == [] and h.ai.classify == []
    assert vals(md, "reused_embeddings", "new_embeddings", "reused_categories", "new_categories") == [6, 0, 6, 0]
    assert h.table_rows() == before                                   # same rows, same values, same ids (atomic replace)
    assert not [m for m in h.logs() if "none of their" in m]          # reuse worked: no 'found 0' warning


def test_one_changed_document_calls_the_ai_only_for_its_chunks(h):
    h.run()
    h.set_docs({**DOCS, "d1": "alpha beta CHANGED"})
    md = h.run()
    assert [t for _, t in h.ai.embed] == ["alpha beta CHANGED"]
    assert [t for t, _ in h.ai.classify] == ["alpha beta CHANGED"]
    assert vals(md, "reused_embeddings", "new_embeddings", "reused_categories", "new_categories") == [5, 1, 5, 1]
    assert "alpha beta gamma" not in h.texts() and "alpha beta CHANGED" in h.texts()
    assert all(r["embedding"] == ai_stubs.vector_for(h.called(h.model), r["chunk_text"]) for r in h.table_rows())


def test_added_and_removed_documents(h):
    h.run()
    docs = {k: v for k, v in DOCS.items() if k != "d2"}
    docs["d5"] = "brand new"
    h.set_docs(docs)
    md = h.run()
    assert [t for _, t in h.ai.embed] == ["brand new"]
    assert len(h.table_rows()) == 4 and vals(md, "reused_embeddings", "new_embeddings") == [3, 1]


def test_embedding_model_change_reembeds_everything_but_keeps_categories(h):
    h.run()
    h.steps[2]["model"] = h.model + "_v2"
    md = h.run()
    assert len(h.ai.embed) == 5 and {m for m, _ in h.ai.embed} == {h.called(h.model + "_v2")} and h.ai.classify == []
    assert vals(md, "reused_embeddings", "new_embeddings", "reused_categories", "new_categories") == [0, 6, 6, 0]
    rows = h.table_rows()
    assert {r["embedding_model"] for r in rows} == {h.model + "_v2"}
    assert all(r["embedding"] == ai_stubs.vector_for(h.called(h.model + "_v2"), r["chunk_text"]) for r in rows)   # no stale vector survived
    assert any("none of their embeddings" in m for m in h.logs())      # prior table was non-empty and nothing was reusable: warned


def test_label_change_reclassifies_everything_but_keeps_embeddings(h):
    h.run()
    sig_before = h.table_rows()[0]["category_signature"]
    h.steps[1]["candidate_labels"] = ["billing", "technical", "other", "spam"]
    md = h.run()
    assert h.ai.embed == [] and len(h.ai.classify) == 5
    assert {labels for _, labels in h.ai.classify} == {("billing", "technical", "other", "spam")}
    assert vals(md, "reused_embeddings", "new_embeddings", "reused_categories", "new_categories") == [6, 0, 0, 6]
    assert {r["category_signature"] for r in h.table_rows()} != {sig_before}
    assert any("none of their categories" in m for m in h.logs())


def test_table_from_an_older_version_without_the_new_columns_falls_back_to_full(h):
    h.run()
    for col in ("embedding_model", "category_signature"):
        h.con.execute(f"ALTER TABLE {h.duck} DROP COLUMN {col}")
    md = h.run()
    assert len(h.ai.embed) == 5 and len(h.ai.classify) == 5            # nothing reusable, full build, no crash
    assert vals(md, "reused_embeddings", "reused_categories") == [0, 0]
    assert {r["embedding_model"] for r in h.table_rows()} == {h.model}  # and the next run can reuse
    h.run()
    assert h.ai.embed == [] and h.ai.classify == []


def test_a_step_added_later_builds_only_that_feature_in_full(h):
    full_steps = h.steps
    h.steps = [full_steps[0], full_steps[2]]                            # first built without classify
    h.run()
    h.steps = full_steps
    md = h.run()
    assert h.ai.embed == [] and len(h.ai.classify) == 5
    assert vals(md, "reused_embeddings", "new_embeddings", "new_categories") == [6, 0, 6]


def test_incremental_false_pays_for_the_whole_corpus_every_run(h):
    h.run(incremental=False)
    h.run(incremental=False)
    assert len(h.ai.embed) == 6 and len(h.ai.classify) == 6            # legacy shape: no dedupe, no reuse
    md = h.run(incremental=False)
    assert "reused_embeddings" not in md
    h.run()                                                            # switching on: the table already carries the reuse columns
    assert h.ai.embed == [] and h.ai.classify == []


def test_embed_only_and_classify_only_pipelines(h):
    for keep in ([0, 2], [0, 1]):
        h.con.execute(f"DROP TABLE IF EXISTS {h.duck}")
        steps = [STEPS_ for i, STEPS_ in enumerate([h.steps[0], h.steps[1], h.steps[2]]) if i in keep]
        h.steps = steps
        h.run()
        h.run()
        assert h.ai.embed == [] and h.ai.classify == []
        h.steps = [dict(s) for s in STEPS]
        h.steps[2]["model"] = h.model


def test_stamped_provenance_is_stable_across_incremental_runs(h):
    h.run(stamp_provenance=True)
    a = h.table_rows()
    h.run(stamp_provenance=True)
    assert h.table_rows() == a and a[0]["source_version"]


def test_mutation_breaking_the_join_key_is_caught(h, monkeypatch):
    """The zero-call assertion must fail if the reuse join no longer matches on content_hash."""
    h.run()
    real = h.mod._build_sql_mode_query

    def broken(*a, **k):
        return real(*a, **k).replace("ON p.content_hash = b.content_hash", "ON p.content_hash = b.chunk_id")

    monkeypatch.setattr(h.mod, "_build_sql_mode_query", broken)
    h.run()
    assert len(h.ai.embed) > 0 and len(h.ai.classify) > 0              # i.e. test_second_run_..._zero_ai_calls would fail


def test_mutation_ignoring_the_model_is_caught(h, monkeypatch):
    h.run()
    real = h.mod._build_sql_mode_query
    monkeypatch.setattr(h.mod, "_build_sql_mode_query", lambda *a, **k: real(*a, **k).replace("embedding_model = ", "embedding_model IS NOT NULL OR embedding_model = "))
    h.steps[2]["model"] = h.model + "_v2"
    h.run()
    assert h.ai.embed == []                                            # the mutant reuses stale vectors...
    assert {r["embedding_model"] for r in h.table_rows()} == {h.model + "_v2"}   # ...stamped with the new model: test_embedding_model_change catches it


# receipts feedback / quarantine apply after reuse, exactly as in the full build

def test_receipts_exclude_quarantine_still_applies_and_excluded_chunks_cost_nothing(mod, scenario):  # noqa: F811
    _seed_matching_source(scenario.client)
    client = scenario.client
    steps = [{"id": "c", "op": "chunk", "chunk_size": 4, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
             {"id": "k", "op": "classify", "candidate_labels": LABELS}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    rc = {"dataset": "proj.rcpt", "min_retrievals": 2, "min_outcomes": 2, "quarantine": {"mean_outcome_below": -0.2, "mode": "exclude"}}

    def run(**kw):
        comp = mod.ContextEngineeringPipelineComponent(
            asset_name="kb", source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body FROM ds.src"}, id_column="id",
            text_column="body", metadata_columns=[], execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb", code_version="v1",
            steps=steps, receipts=rc, **kw)
        client.ai.reset()
        res = dg.materialize([list(comp.build_defs(None).assets)[0]], resources={"bq": FakeBigQueryResource(client)}, instance=dg.DagsterInstance.ephemeral())
        assert res.success
        return res.get_asset_materialization_events()[0].materialization.metadata

    full = run(incremental=False)
    full_ids = [r["chunk_id"] for r in client.query("SELECT chunk_id FROM ds.kb ORDER BY chunk_id").result()]
    n_full_calls = len(client.ai.embed)
    first = run()
    assert [r["chunk_id"] for r in client.query("SELECT chunk_id FROM ds.kb ORDER BY chunk_id").result()] == full_ids   # same survivors
    assert len(client.ai.embed) <= n_full_calls
    second = run()
    assert client.ai.embed == [] and client.ai.classify == []
    assert second["reused_embeddings"].value == second["dagster/row_count"].value == len(full_ids)
    assert second["receipts_quarantine_mode"].value == "exclude" and full["receipts_quarantine_mode"].value == "exclude"
    assert first["dagster/row_count"].value == full["dagster/row_count"].value


# ── parse-only: every statement shape, in its own dialect ───────────────────────────────────────────────

def _build(mod, dialect, steps, **kw):
    return mod._build_sql_mode_query(steps, {"sql": "SELECT id, body, cust FROM ds.src"}, dialect, "ds.kb", "id", "body", ["cust"],
                                     asset_name="kb", code_version="v1", incremental=True, **kw)


SHAPES = [("both", [0, 1, 2]), ("embed_only", [0, 2]), ("classify_only", [0, 1])]


@pytest.mark.parametrize("dialect", list(READ))
@pytest.mark.parametrize("shape", [s for s, _ in SHAPES])
@pytest.mark.parametrize("reuse_embed,reuse_classify", list(itertools.product([False, True], repeat=2)))
@pytest.mark.parametrize("stamp,receipts", [(False, False), (True, False), (True, True)])
def test_every_incremental_shape_parses_in_its_dialect_and_calls_ai_only_on_new_rows(mod, dialect, shape, reuse_embed, reuse_classify, stamp, receipts):
    steps = [STEPS[i] for i in dict(SHAPES)[shape]]
    kw = {}
    if receipts:
        kw["receipts"] = mod._validate_receipts({"dataset": "DB.SCH" if dialect != "bigquery" else "proj.rcpt", "quarantine": {"mode": "exclude"}},
                                                [], "sql", dialect, {"resource_key": "wh"})
    sql = _build(mod, dialect, steps, stamp_provenance=stamp, reuse={"embed": reuse_embed, "classify": reuse_classify}, **kw)
    tree = sqlglot.parse_one(sql, read=READ[dialect])
    counts = _build(mod, dialect, steps, stamp_provenance=stamp, reuse={"embed": reuse_embed, "classify": reuse_classify}, counts_only=True, **kw)
    ctree = sqlglot.parse_one(counts, read=READ[dialect])
    ai_names = {"ml.generate_embedding", "ai.classify", "snowflake.cortex.classify_text", "snowflake.cortex.embed_text_768", "ai_query", "ai_classify"}
    for node in tree.walk():
        text = node.sql(dialect=READ[dialect]).lower() if isinstance(node, (exp.Anonymous, exp.Dot, exp.GenerateEmbedding)) else ""
        if any(text.startswith(n) for n in ai_names) or isinstance(node, exp.GenerateEmbedding):
            cte = node.find_ancestor(exp.CTE)
            assert cte is not None and cte.alias.startswith("new_"), f"AI call outside a new_* CTE: {text[:60]}"   # only the todo subset reaches it
    assert not any(isinstance(n, exp.GenerateEmbedding) for n in ctree.walk()), "counts_only must not call the AI function"
    assert "ai_query" not in counts and "CLASSIFY" not in counts.upper().replace("CLASSIFIED", "")
    sql = sql.replace("`ds.kb`", "ds.kb")      # BigQuery tables are always emitted as one backticked path
    out_tbl_refs = sql.count("FROM ds.kb")
    assert out_tbl_refs == int(reuse_embed and "embed" in shape) + int(reuse_classify and "classify" in shape) + int(shape == "both" and reuse_embed and reuse_classify) * 0 \
        or shape == "both"
    assert (out_tbl_refs > 0) == ((reuse_embed and shape != "classify_only") or (reuse_classify and shape != "embed_only"))
    assert "REPLACE TABLE ds.kb" in sql


def test_probe_reports_missing_and_incomplete_tables_per_dialect(mod, h):
    cls = h.steps[1]
    emb = h.steps[2]
    from types import SimpleNamespace
    ctx = SimpleNamespace(log=SimpleNamespace(warning=lambda m: None, info=lambda m: None))
    with mod._sql_session(h.resource, "wh") as s:
        assert mod._sql_probe_prior_table(s, h.dialect, h.table, cls, emb, ctx) == ({"embed": False, "classify": False, "enrich": False}, 0)
    h.run()
    with mod._sql_session(h.resource, "wh") as s:
        assert mod._sql_probe_prior_table(s, h.dialect, h.table, cls, emb, ctx) == ({"embed": True, "classify": True, "enrich": False}, 6)
        assert mod._sql_probe_prior_table(s, h.dialect, h.table, None, emb, ctx)[0] == {"embed": True, "classify": False, "enrich": False}
