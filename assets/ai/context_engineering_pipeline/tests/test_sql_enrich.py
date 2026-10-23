"""execution_mode='sql': enrich (an LLM-written field on every chunk), mapped onto each warehouse's own
text-generation AI function: SNOWFLAKE.CORTEX.COMPLETE, Databricks ai_query (the SAME function already
used for embed there -- see ai_stubs.py's own docstring for how the real-execution stub tells the two
apart), and BigQuery ML.GENERATE_TEXT. Snowflake and Databricks are executed for real (sqlglot -> DuckDB,
same harness as the rest of execution_mode='sql'); BigQuery is checked structurally only -- ML.GENERATE_TEXT /
AI.GENERATE_TEXT has no sqlglot support for its MODEL/TABLE/STRUCT table-function syntax as of sqlglot
28.0.0 (confirmed directly: sqlglot.parse_one raises ParseError on both spellings), so there is no fake
client to execute it against; the generated SQL is instead checked against the real BigQuery docs by eye.
Not supported, rejected with a clear error instead of silently doing the wrong thing: prepend=True (would
need content_hash re-hashed mid-statement, same reason sensitivity's action='redact' is rejected)."""
import dagster as dg
import pytest

from .test_sql_incremental import READ, STEPS, Harness, vals


@pytest.fixture()
def mod():
    from .conftest import load_component_module
    return load_component_module()


@pytest.fixture(params=["snowflake_cortex", "databricks"])
def h(request, mod):
    harness = Harness(mod, request.param)
    yield harness
    harness.wh.close()


ENRICH_STEP = {"id": "n", "op": "enrich", "mode": "summary"}


def test_enrich_real_execution_calls_the_model_once_per_distinct_chunk(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model}]
    h.steps = steps
    md = h.run()
    rows = h.table_rows()
    assert len(rows) == 6
    assert all(r["enrichment_summary"] for r in rows)
    assert all(r["enrichment_signature"] == f"{h.model}:summary" for r in rows)
    # distinct chunk text is enriched once (same content-hash dedup as classify/embed); "new_enrichments"
    # counts ROWS that pick up a value (same convention as new_embeddings/new_categories), not AI calls
    distinct_texts = {r["chunk_text"] for r in rows}
    assert len(h.ai.enrich) == len(distinct_texts)
    assert vals(md, "reused_enrichments", "new_enrichments") == [0, 6]


def test_enrich_second_run_with_unchanged_corpus_makes_zero_calls(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model}]
    h.steps = steps
    h.run()
    before = h.table_rows()
    md = h.run()
    assert h.ai.enrich == []
    assert h.table_rows() == before
    assert vals(md, "reused_enrichments") == [6]


def test_enrich_model_change_recomputes_everything(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model}]
    h.steps = steps
    h.run()
    steps2 = [STEPS[0], {**ENRICH_STEP, "model": h.model + "_v2"}]
    h.steps = steps2
    h.run()
    assert len(h.ai.enrich) > 0
    assert {r["enrichment_signature"] for r in h.table_rows()} == {f"{h.model}_v2:summary"}


def test_enrich_mode_change_recomputes_everything(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model}]
    h.steps = steps
    h.run()
    steps2 = [STEPS[0], {**ENRICH_STEP, "model": h.model, "mode": "keywords"}]
    h.steps = steps2
    h.run()
    assert len(h.ai.enrich) > 0
    assert {r["enrichment_signature"] for r in h.table_rows()} == {f"{h.model}:keywords"}


def test_enrich_custom_output_column(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model, "output_column": "my_summary"}]
    h.steps = steps
    h.run()
    rows = h.table_rows()
    assert all("my_summary" in r for r in rows)
    assert "enrichment_summary" not in rows[0]


def test_enrich_combined_with_classify_and_embed(h):
    steps = [STEPS[0], {**ENRICH_STEP, "model": h.model}, STEPS[1], STEPS[2]]
    h.steps = steps
    md = h.run()
    rows = h.table_rows()
    assert len(rows) == 6
    assert all(r["enrichment_summary"] and r["category"] and r["embedding"] for r in rows)
    assert vals(md, "new_enrichments", "new_categories", "new_embeddings") == [6, 6, 6]  # row counts, not distinct-chunk counts
    distinct_texts = {r["chunk_text"] for r in rows}
    assert len(h.ai.enrich) == len(h.ai.classify) == len(distinct_texts)  # d3/d4 share one distinct chunk text -> 5 AI calls each


def test_enrich_prepend_rejected(h):
    h.steps = [STEPS[0], {**ENRICH_STEP, "model": h.model, "prepend": True}]
    with pytest.raises(ValueError, match="prepend=True is not supported"):
        h.run()


def test_enrich_requires_model(mod):
    # These validations live inside _build_sql_mode_query (called lazily when the asset materializes,
    # same as classify's candidate_labels check), not at build_defs() time -- called directly here,
    # same as the BigQuery structural tests below.
    steps = [{"id": "c", "op": "chunk"}, {"id": "n", "op": "enrich", "mode": "summary"}]
    with pytest.raises(ValueError, match="enrich op requires `model`"):
        mod._build_sql_mode_query(steps, {"sql": "select 1"}, "snowflake_cortex", "db.s.t", "id", "body", [],
                                   asset_name="kb", code_version="v1")


def test_enrich_rejects_invalid_mode(mod):
    steps = [{"id": "c", "op": "chunk"}, {"id": "n", "op": "enrich", "mode": "nope", "model": "m"}]
    with pytest.raises(ValueError, match="mode must be one of"):
        mod._build_sql_mode_query(steps, {"sql": "select 1"}, "snowflake_cortex", "db.s.t", "id", "body", [],
                                   asset_name="kb", code_version="v1")


def test_per_step_ops_sql_rejects_enrich(mod):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="bigquery",
        output_table="ds.kb", per_step_ops=True,
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT * FROM t"},
        steps=[{"id": "c", "op": "chunk"}, {"id": "n", "op": "enrich", "mode": "summary", "model": "m"}],
    )
    with pytest.raises(ValueError, match="aren't available as their own per-step SQL tables"):
        comp.build_defs(context=None)


# ── BigQuery: structural only, no sqlglot/DuckDB (see module docstring) ───────────────────────────

def test_enrich_bigquery_structural_shape(mod):
    steps = [STEPS[0], {"id": "n", "op": "enrich", "mode": "context", "model": "p.d.gen1"}]
    sql = mod._build_sql_mode_query(
        steps, {"sql": "SELECT id, body FROM ds.src"}, "bigquery", "proj.ds.kb", "id", "body", [],
        asset_name="kb", code_version="v1", incremental=True,
    )
    assert "ML.GENERATE_TEXT(MODEL `p.d.gen1`," in sql
    assert "STRUCT(TRUE AS flatten_json_output)" in sql
    assert "ml_generate_text_llm_result AS enrichment_context" in sql
    assert "AS prompt FROM todo_enrichment" in sql
    counts_sql = mod._build_sql_mode_query(
        steps, {"sql": "SELECT id, body FROM ds.src"}, "bigquery", "proj.ds.kb", "id", "body", [],
        asset_name="kb", code_version="v1", incremental=True, counts_only=True,
    )
    assert "new_enrichments" in counts_sql
    assert "ML.GENERATE_TEXT" not in counts_sql  # counts_only must not call the AI function


def test_enrich_bigquery_rejects_malformed_model(mod):
    steps = [{"id": "c", "op": "chunk"}, {"id": "n", "op": "enrich", "mode": "summary", "model": "not a model ref!"}]
    with pytest.raises(ValueError, match="enrich model .* is not a valid BigQuery model reference"):
        mod._build_sql_mode_query(steps, {"sql": "SELECT * FROM t"}, "bigquery", "ds.kb", "id", "body", [],
                                   asset_name="kb", code_version="v1")


@pytest.mark.parametrize("shape", ["enrich_only", "classify_then_enrich_then_embed", "enrich_then_embed"])
def test_enrich_bigquery_non_incremental_structural_shapes(mod, shape):
    steps_by_shape = {
        "enrich_only": [STEPS[0], {"id": "n", "op": "enrich", "mode": "summary", "model": "p.d.gen1"}],
        "classify_then_enrich_then_embed": [STEPS[0], STEPS[1], {"id": "n", "op": "enrich", "mode": "summary", "model": "p.d.gen1"}, STEPS[2]],
        "enrich_then_embed": [STEPS[0], {"id": "n", "op": "enrich", "mode": "summary", "model": "p.d.gen1"}, STEPS[2]],
    }
    steps = steps_by_shape[shape]
    steps = [dict(s, model="p.d.m1") if s["op"] == "embed" else s for s in steps]
    sql = mod._build_sql_mode_query(
        steps, {"sql": "SELECT id, body FROM ds.src"}, "bigquery", "proj.ds.kb", "id", "body", [],
        asset_name="kb", code_version="v1", incremental=False,
    )
    assert sql.count("ML.GENERATE_TEXT(MODEL `p.d.gen1`,") == 1
    if shape == "classify_then_enrich_then_embed":
        assert "AS category" in sql and sql.count("ML.GENERATE_EMBEDDING(MODEL `p.d.m1`,") == 1
    if "embed" in shape or "then_embed" in shape:
        assert "ml_generate_embedding_result AS embedding" in sql
    else:
        assert "ml_generate_text_llm_result AS enrichment_summary" in sql  # enrich is the final table function
