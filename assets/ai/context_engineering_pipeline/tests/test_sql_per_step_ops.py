"""execution_mode='sql' + per_step_ops=True: one real warehouse table (and one real asset) per step.

Executed here through the exact same DuckDB-backed harness as test_sql_incremental.py (FakeBigQueryClient/
FakeBigQueryResource, sqlglot -> DuckDB, real dg.materialize) -- this is the live-executed counterpart to
test_context_engineering_pipeline.py::test_per_step_ops_sql_rejects_partitioning, which only checks the one
remaining restriction (partitioning) structurally."""
import dagster as dg
import pytest

from .conftest import load_component_module
from .test_sql_incremental import DOCS, LABELS, STEPS, Harness, vals


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def h(mod):
    harness = Harness(mod, "bigquery")
    yield harness
    harness.wh.close()


def _materialize(harness, **kw):
    asset_def = list(harness.component(per_step_ops=True, **kw).build_defs(None).assets)[0]
    harness.ai.reset()
    instance = dg.DagsterInstance.ephemeral()
    res = dg.materialize([asset_def], resources={"wh": harness.resource}, instance=instance)
    assert res.success
    harness.last = (res, instance)
    return res, asset_def


def _md_by_key(res):
    return {
        e.materialization.asset_key.to_user_string(): e.materialization.metadata
        for e in res.get_asset_materialization_events()
    }


def test_publishes_one_real_table_per_step_with_real_lineage(h):
    res, asset_def = _materialize(h)

    deps = {k.to_user_string(): {d.to_user_string() for d in v} for k, v in asset_def.asset_deps.items()}
    assert deps == {"kb_c": set(), "kb_k": {"kb_c"}, "kb": {"kb_k"}}   # chunk has no upstream asset in sql mode; last step keeps the plain asset_name

    md = _md_by_key(res)
    assert set(md) == {"kb_c", "kb_k", "kb"}

    # Three distinct, real physical tables -- not one statement wearing three asset names.
    chunks_rows = h.con.execute("SELECT * FROM ds.kb__c ORDER BY chunk_id").fetchall()
    classified_rows = h.con.execute("SELECT * FROM ds.kb__k ORDER BY chunk_id").fetchall()
    final_rows = h.table_rows()
    assert len(chunks_rows) == 6 and len(classified_rows) == 6 and len(final_rows) == 6
    classified_cols = [d[0] for d in h.con.execute("SELECT * FROM ds.kb__k LIMIT 0").description]
    assert "category" in classified_cols and "embedding" not in classified_cols
    assert all(r["category"] in LABELS for r in final_rows) and all(len(r["embedding"]) == 3 for r in final_rows)

    # Cross-cutting metadata lands on the FINAL step only.
    assert "step_chain" in md["kb"] and "n_steps" in md["kb"]
    assert "step_chain" not in md["kb_c"]
    assert md["kb_c"]["sql_table"].value == f"{h.table}__c"
    assert md["kb_k"]["sql_table"].value == f"{h.table}__k"


def test_incremental_reuse_still_skips_ai_calls_on_the_second_run(h):
    _materialize(h)
    before = h.table_rows()
    res, _ = _materialize(h)
    assert h.ai.embed == [] and h.ai.classify == []
    md = _md_by_key(res)
    assert vals(md["kb_k"], "reused") == [True]
    assert vals(md["kb"], "reused") == [True]
    assert h.table_rows() == before


def test_one_changed_document_only_reaches_the_ai_for_its_own_chunks(h):
    _materialize(h)
    h.set_docs({**DOCS, "d1": "alpha beta CHANGED"})
    _materialize(h)
    assert [t for _, t in h.ai.embed] == ["alpha beta CHANGED"]
    assert [t for t, _ in h.ai.classify] == ["alpha beta CHANGED"]
    assert "alpha beta CHANGED" in h.texts() and "alpha beta gamma" not in h.texts()


def test_retrieval_eval_check_attaches_to_the_final_step_asset(h):
    """The last step's op is the only one that may also yield AssetCheckResults (retrieval_eval /
    regression_check both describe 'the built index', i.e. this step specifically). Executed through
    the implicit global job, matching this repo's own convention that dg.materialize() alone does not
    run checks."""
    cfg = {"queries": [{"query": "alpha beta gamma", "expected": ["d1"]}], "k": 3, "min_recall": 1.0}
    defs = dg.Definitions.merge(
        h.component(per_step_ops=True, retrieval_eval=cfg).build_defs(None),
        dg.Definitions(resources={"wh": h.resource}),
    )
    res = defs.get_implicit_global_asset_job_def().execute_in_process(instance=dg.DagsterInstance.ephemeral())
    assert res.success
    evs = {e.check_name: e for e in res.get_asset_check_evaluations()}
    ev = evs["retrieval_quality"]
    assert ev.asset_key.to_user_string() == "kb" and ev.passed
    assert ev.metadata["recall_at_k"].value == 1.0


def test_embed_only_pipeline_has_no_classify_table(h):
    h.con.execute("DROP TABLE IF EXISTS ds.kb")
    only = [STEPS[0], STEPS[2]]
    _materialize(h, steps=only)
    assert not h.con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='ds' AND table_name='kb__k'"
    ).fetchall()
    rows = h.table_rows()
    assert len(rows) == 6 and "embedding" in rows[0] and "category" not in rows[0]
