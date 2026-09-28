"""Committed regression tests for ContextEngineeringPipelineComponent.

Split by what's actually live-tested vs. structural-only:

- The full python-mode pipeline (chunk -> classify -> embed -> write_vector_store)
  runs for REAL against sentence-transformers + HuggingFace zero-shot + chromadb --
  nothing mocked, matching this repo's "free/local by default" testability
  convention for this component family.
- `source: {kind: warehouse_query}` is tested for REAL against DuckDB
  (dagster-duckdb), since that's a genuinely free, local warehouse.
- `provider='litellm'` (embed) and `mode='llm'` (classify) are tested with
  litellm mocked -- there is no free/local litellm backend.
- `execution_mode='sql'` has NO live warehouse available in this environment,
  so it is tested structurally only: the generated SQL string is asserted,
  never executed. See README's Validation section.
"""
import json
import os
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    load_component_module,
    requires_chromadb,
    requires_duckdb,
    requires_sentence_transformers,
    requires_transformers,
)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def tickets_df():
    return pd.DataFrame({
        "ticket_id": ["T1", "T2", "T3"],
        "customer_id": ["C1", "C2", "C3"],
        "body": [
            "I was charged twice for my subscription this month, please refund the duplicate charge.",
            "The app crashes every time I try to upload a photo larger than 5MB, here are the logs.",
            "My package says delivered but I never received it, tracking number is 1Z999AA10123456784.",
        ],
    })


def _materialize(component, df=None, upstream_name="raw_tickets", resources=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assets = [asset_def]
    if df is not None:
        @dg.asset(name=upstream_name)
        def _upstream():
            return df
        assets.append(_upstream)
    return dg.materialize(assets, resources=resources or {}, raise_on_error=False)


# ── real, end-to-end python-mode pipeline ────────────────────────────────

@requires_sentence_transformers
@requires_transformers
@requires_chromadb
def test_full_pipeline_chunk_classify_embed_write_vector_store(mod, tickets_df, tmp_path):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="support_kb",
        upstream_asset_key="raw_tickets",
        id_column="ticket_id",
        text_column="body",
        metadata_columns=["customer_id"],
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 200},
            {"id": "classified", "op": "classify", "candidate_labels": ["billing", "technical", "shipping"]},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "support_tickets"},
        ],
    )
    result = _materialize(component, tickets_df)
    assert result.success
    df_out = result.output_for_node("support_kb")
    assert len(df_out) == 3
    assert set(df_out["category"]) <= {"billing", "technical", "shipping"}
    assert "embedding" in df_out.columns

    import chromadb
    client = chromadb.PersistentClient(path=str(tmp_path / "chroma"))
    collection = client.get_collection("support_tickets")
    assert collection.count() == 3
    hits = collection.query(query_texts=["refund for a duplicate charge"], n_results=1)
    assert hits["ids"][0][0] in {"T1_0"}


@requires_sentence_transformers
@requires_transformers
def test_chunking_explodes_rows_and_carries_metadata(mod, tickets_df):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="chunks_only",
        upstream_asset_key="raw_tickets",
        id_column="ticket_id",
        text_column="body",
        metadata_columns=["customer_id"],
        steps=[{"id": "chunks", "op": "chunk", "chunk_size": 40, "chunk_overlap": 5}],
    )
    result = _materialize(component, tickets_df)
    assert result.success
    df_out = result.output_for_node("chunks_only")
    assert len(df_out) > 3
    assert set(df_out["ticket_id"]) == {"T1", "T2", "T3"}
    assert "customer_id" in df_out.columns
    assert (df_out.groupby("ticket_id")["chunk_index"].apply(lambda s: list(s) == list(range(len(s))))).all()


# ── source: warehouse_query, real DuckDB ─────────────────────────────────

@requires_duckdb
@requires_sentence_transformers
def test_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "tickets.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE tickets AS SELECT * FROM (VALUES "
        "(1, 'Refund my duplicate charge please'), "
        "(2, 'App crashes on upload, logs attached')) AS t(ticket_id, body)"
    )
    conn.close()

    component = mod.ContextEngineeringPipelineComponent(
        asset_name="support_kb_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM tickets"},
        id_column="ticket_id",
        text_column="body",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 200},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
        ],
    )
    result = _materialize(component, resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("support_kb_sql_source")
    assert len(df_out) == 2
    assert set(df_out["ticket_id"]) == {1, 2}


# ── max_source_rows guardrail ─────────────────────────────────────────────

@requires_sentence_transformers
def test_max_source_rows_truncates_and_reports_both_counts(mod):
    df = pd.DataFrame({"id": range(10), "txt": [f"row {i} content" for i in range(10)]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="capped",
        upstream_asset_key="src",
        id_column="id",
        text_column="txt",
        max_source_rows=3,
        steps=[{"id": "c", "op": "chunk", "chunk_size": 500}],
    )
    result = _materialize(component, df, upstream_name="src")
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["source_rows"].value == 3
    assert meta["source_rows_available"].value == 10


# ── litellm-backed embed / classify (mocked -- no free/local backend) ────

def test_embed_provider_litellm_calls_litellm_embedding(mod):
    df = pd.DataFrame({"chunk_text": ["a", "b"]})
    fake_resp = MagicMock()
    fake_resp.data = [{"embedding": [0.1, 0.2]}, {"embedding": [0.3, 0.4]}]
    with patch("litellm.embedding", return_value=fake_resp) as mock_embed:
        out = mod._do_embed(df, {"provider": "litellm", "model": "text-embedding-3-small"}, _FakeContext())
    mock_embed.assert_called_once()
    assert out["embedding"].tolist() == [[0.1, 0.2], [0.3, 0.4]]


def test_classify_mode_llm_calls_litellm_completion_and_validates_category(mod):
    df = pd.DataFrame({"chunk_text": ["I was overcharged"]})
    fake_message = MagicMock()
    fake_message.content = '{"category": "billing"}'
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=fake_message)]
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        out = mod._do_classify(
            df,
            {"mode": "llm", "candidate_labels": ["billing", "technical"], "model": "gpt-4o-mini"},
            _FakeContext(),
        )
    mock_completion.assert_called_once()
    assert out["category"].tolist() == ["billing"]


def test_classify_mode_llm_rejects_category_outside_candidate_labels(mod):
    df = pd.DataFrame({"chunk_text": ["something"]})
    fake_message = MagicMock()
    fake_message.content = '{"category": "not_a_real_label"}'
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=fake_message)]
    with patch("litellm.completion", return_value=fake_resp):
        out = mod._do_classify(
            df, {"mode": "llm", "candidate_labels": ["billing", "technical"]}, _FakeContext(),
        )
    assert out["category"].tolist() == [None]


class _FakeContext:
    class log:
        @staticmethod
        def info(msg): pass
        @staticmethod
        def warning(msg): pass


# ── validation guards ──────────────────────────────────────────────────

def test_mutual_exclusivity_upstream_and_source(mod):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="a",
        upstream_asset_key="up",
        source={"kind": "warehouse_query", "resource_key": "r", "sql": "s"},
        id_column="id", text_column="txt",
        steps=[{"id": "c", "op": "chunk"}],
    )
    with pytest.raises(ValueError, match="set exactly one"):
        component.build_defs(context=None)


def test_requires_one_of_upstream_or_source(mod):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="a", id_column="id", text_column="txt",
        steps=[{"id": "c", "op": "chunk"}],
    )
    with pytest.raises(ValueError, match="set one of"):
        component.build_defs(context=None)


def test_invalid_execution_mode_raises(mod):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="a", upstream_asset_key="up", id_column="id", text_column="txt",
        execution_mode="bogus", steps=[{"id": "c", "op": "chunk"}],
    )
    with pytest.raises(ValueError, match="execution_mode must be"):
        component.build_defs(context=None)


@pytest.mark.parametrize("missing_field,kwargs,match", [
    ("sql_dialect", dict(source={"kind": "warehouse_query", "resource_key": "r", "sql": "s"}, execution_mode="sql"), "sql_dialect"),
    ("output_table", dict(source={"kind": "warehouse_query", "resource_key": "r", "sql": "s"}, execution_mode="sql", sql_dialect="snowflake_cortex"), "output_table"),
    ("source", dict(upstream_asset_key="up", execution_mode="sql", sql_dialect="snowflake_cortex", output_table="t"), "requires"),
])
def test_sql_mode_requires_dialect_table_and_sql_source(mod, missing_field, kwargs, match):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="a", id_column="id", text_column="txt",
        steps=[{"id": "c", "op": "chunk"}], **kwargs,
    )
    with pytest.raises(ValueError, match=match):
        component.build_defs(context=None)


def test_first_step_must_be_chunk(mod):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="a", upstream_asset_key="up", id_column="id", text_column="txt",
        steps=[{"id": "e", "op": "embed"}],
    )
    with pytest.raises(ValueError, match="first step must be op='chunk'"):
        component.build_defs(context=None)


# ── execution_mode='sql': structural only, no live warehouse available ──

def test_sql_mode_generates_valid_snowflake_cortex_sql(mod):
    sql = mod._build_sql_mode_query(
        steps_cfg=[
            {"id": "chunks", "op": "chunk", "chunk_size": 500, "chunk_overlap": 50},
            {"id": "classified", "op": "classify", "candidate_labels": ["billing", "technical"]},
            {"id": "embedded", "op": "embed", "model": "snowflake-arctic-embed-m"},
        ],
        source_cfg={"kind": "warehouse_query", "resource_key": "sf", "sql": "SELECT ticket_id, body, customer_id FROM tickets"},
        dialect="snowflake_cortex",
        output_table="analytics.support_kb",
        id_column="ticket_id", text_column="body", metadata_columns=["customer_id"],
    )
    assert "CREATE OR REPLACE TABLE analytics.support_kb AS" in sql
    assert "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER" in sql
    assert "SNOWFLAKE.CORTEX.CLASSIFY_TEXT" in sql
    assert "SNOWFLAKE.CORTEX.EMBED_TEXT_768" in sql
    assert "src.customer_id" in sql


def test_sql_mode_generates_valid_bigquery_sql(mod):
    sql = mod._build_sql_mode_query(
        steps_cfg=[{"id": "chunks", "op": "chunk"}, {"id": "embedded", "op": "embed"}],
        source_cfg={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, txt FROM t"},
        dialect="bigquery", output_table="ds.kb", id_column="id", text_column="txt", metadata_columns=[],
    )
    assert "ML.GENERATE_EMBEDDING" in sql


def test_sql_mode_generates_valid_databricks_sql(mod):
    sql = mod._build_sql_mode_query(
        steps_cfg=[{"id": "chunks", "op": "chunk"}, {"id": "embedded", "op": "embed"}],
        source_cfg={"kind": "warehouse_query", "resource_key": "dbx", "sql": "SELECT id, txt FROM t"},
        dialect="databricks", output_table="ds.kb", id_column="id", text_column="txt", metadata_columns=[],
    )
    assert "ai_query" in sql


def test_sql_mode_classify_without_candidate_labels_raises(mod):
    with pytest.raises(ValueError, match="candidate_labels"):
        mod._build_sql_mode_query(
            steps_cfg=[{"id": "chunks", "op": "chunk"}, {"id": "classified", "op": "classify"}],
            source_cfg={"kind": "warehouse_query", "resource_key": "x", "sql": "SELECT 1"},
            dialect="snowflake_cortex", output_table="t", id_column="id", text_column="txt", metadata_columns=[],
        )
