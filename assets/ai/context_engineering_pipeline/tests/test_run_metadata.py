"""Run metadata the chunk/embed/write_vector_store steps now emit -- document_count,
chunk_count, chunking_settings, sample_chunks (chunk op); embedding_model,
embedding_tokens, embedding_time_seconds (embed op); store_type (write_vector_store op).
Without these, a UI rendering "list of documents" / "chunk preview" views has nothing to
read (the feedback this was built from). chunk/write_vector_store are exercised for real
(sentence-transformers + chromadb, this component's own free/local testability
convention); embed's token-accounting is exercised directly against `_do_embed`/
`_do_embed_raw` with litellm mocked, since there's no free/local provider that reports
token usage.
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb, requires_sentence_transformers


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, df=None, upstream_name="raw_docs", resources=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assets = [asset_def]
    if df is not None:
        @dg.asset(name=upstream_name)
        def _upstream():
            return df
        assets.append(_upstream)
    return dg.materialize(assets, resources=resources or {}, raise_on_error=False)


class _FakeContext:
    class log:
        @staticmethod
        def info(msg): pass
        @staticmethod
        def warning(msg): pass


@requires_sentence_transformers
@requires_chromadb
def test_chunk_step_emits_document_chunk_counts_settings_and_sample(mod, tmp_path):
    df = pd.DataFrame({
        "doc_id": ["A", "B"],
        "txt": [
            "Document A's full text, long enough to split into more than one chunk easily given a small chunk_size.",
            "Document B's full text, equally long so it also produces more than one real chunk for this test.",
        ],
    })
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="test_kb_collection",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="txt",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 40, "chunk_overlap": 5},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "test_kb_collection"},
        ],
    )
    result = _materialize(component, df)
    assert result.success
    # Single-asset (default, non-per_step_ops) mode: one materialization event for the whole
    # pipeline, with the chunk step's own run metadata promoted to the top level (see
    # _chunk_run_metadata / its call site in _asset_body) -- not nested per-step like
    # per_step_ops=True's separate per-step assets would be.
    chunk_meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert chunk_meta["document_count"].value == 2
    assert chunk_meta["chunks_created"].value > 0
    settings = chunk_meta["chunking_settings"].data
    assert settings["chunk_size"] == 40
    assert settings["chunk_overlap"] == 5
    assert settings["strategy"] == "fixed"  # the op's own default, confirmed present even when unset in YAML
    # sample_chunks is real JSON (sampled across documents, full chunk_text, not a markdown
    # table -- a chunk whose text contains a literal "|" would otherwise break a markdown
    # table's columns), with a separate markdown preview under its own key.
    sample = chunk_meta["sample_chunks"].data
    assert isinstance(sample, list) and len(sample) > 0
    assert {"doc_id", "chunk_id", "chunk_index", "chunk_text"} <= set(sample[0])
    assert set(row["doc_id"] for row in sample) == {"A", "B"}  # sampled across both documents, not just the first
    assert chunk_meta["sample_chunks_preview"].value  # the markdown version still exists, under its own key


@requires_sentence_transformers
@requires_chromadb
def test_write_vector_store_step_emits_store_type(mod, tmp_path):
    df = pd.DataFrame({"doc_id": ["A"], "txt": ["A single short document for this test."]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="kb2",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="txt",
        steps=[
            {"id": "chunks", "op": "chunk"},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma2"), "collection_name": "test_kb2_collection"},
        ],
    )
    result = _materialize(component, df)
    assert result.success
    last_meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert last_meta["store_type"].value == "chromadb"


@requires_sentence_transformers
@requires_chromadb
def test_chunks_indexed_step_order_and_document_summary(mod, tmp_path):
    df = pd.DataFrame({
        "doc_id": ["A", "B"],
        "txt": [
            "Document A's full text, long enough to split into more than one chunk easily given a small chunk_size.",
            "Document B's full text, equally long so it also produces more than one real chunk for this test.",
        ],
    })
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="test_kb3",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="txt",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 40, "chunk_overlap": 5},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma3"), "collection_name": "test_kb3_collection"},
        ],
    )
    result = _materialize(component, df)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata

    assert meta["chunks_indexed"].value == meta["dagster/row_count"].value
    assert meta["step_order"].data == ["chunks", "embedded", "indexed"]

    summary = meta["document_summary"].data
    assert {row["doc_id"] for row in summary} == {"A", "B"}
    assert all(row["status"] == "indexed" and row["chunks"] > 0 and row["reason"] is None for row in summary)
    totals = meta["document_summary_totals"].data
    assert totals == {"total_documents": 2, "truncated": False, "indexed": 2, "skipped": 0, "failed": 0}

    # embedding stats are real numeric MetadataValue types, not stringified text, and
    # embedding_tokens is OMITTED (sentence_transformers has no token-billing concept) rather
    # than present as a literal "none" string.
    assert meta["embedding_time_seconds"].value >= 0
    assert isinstance(meta["embedding_time_seconds"].value, float)
    assert "embedding_tokens" not in meta

    # Stable marker tag, independent of `kinds`.
    asset_def = list(component.build_defs(context=None).assets)[0]
    _key = next(iter(asset_def.keys))
    assert asset_def.specs_by_key[_key].tags.get("dagster-community/knowledge-base") == "true"


@requires_sentence_transformers
@requires_chromadb
def test_document_summary_marks_a_fully_filtered_document_skipped(mod, tmp_path):
    df = pd.DataFrame({
        "doc_id": ["A", "B"],
        "txt": [
            "Document A's full text, long enough to split into more than one chunk easily given a small chunk_size.",
            "short",  # too short to survive a min_chunk_chars filter below
        ],
    })
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="test_kb4",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="txt",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0},
            {"id": "filtered", "op": "filter", "text_column": "chunk_text", "min_chars": 20},
            {"id": "embedded", "op": "embed", "provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma4"), "collection_name": "test_kb4_collection"},
        ],
    )
    result = _materialize(component, df)
    assert result.success
    summary = {row["doc_id"]: row for row in result.get_asset_materialization_events()[-1].materialization.metadata["document_summary"].data}
    assert summary["A"]["status"] == "indexed"
    assert summary["B"]["status"] == "skipped"
    assert summary["B"]["chunks"] == 0
    assert summary["B"]["reason"] == "filtered_or_deduped_after_chunking"


def test_do_embed_raw_litellm_reports_tokens_model_and_time(mod):
    fake_resp = MagicMock()
    fake_resp.data = [{"embedding": [0.1, 0.2]}, {"embedding": [0.3, 0.4]}]
    fake_resp.usage.total_tokens = 7
    df = pd.DataFrame({"chunk_text": ["a", "b"]})
    with patch("litellm.embedding", return_value=fake_resp):
        out = mod._do_embed_raw(df, {"provider": "litellm", "model": "m"}, _FakeContext())
    stats = out.attrs["embedding_stats"]
    assert stats["embedding_tokens"] == 7
    assert stats["embedding_model"] == "m"
    assert stats["embedding_time_seconds"] >= 0


def test_do_embed_raw_litellm_mocked_response_without_usage_reports_tokens_as_none(mod):
    """A bare MagicMock() response (this repo's own test_receipts_feedback.py fixture, and
    plausibly a real litellm-proxied provider that just doesn't report usage) must never
    silently produce a non-numeric "token count" -- caught as a real regression: it
    crashed every receipts-feedback test with 'MagicMock is not JSON serializable' when
    first implemented, from `total_tokens += resp.usage.total_tokens` trusting a MagicMock
    like a real number (MagicMock supports __add__/__radd__, so it doesn't even raise)."""
    fake_resp = MagicMock()
    fake_resp.data = [{"embedding": [0.1, 0.2]}]
    df = pd.DataFrame({"chunk_text": ["a"]})
    with patch("litellm.embedding", return_value=fake_resp):
        out = mod._do_embed_raw(df, {"provider": "litellm", "model": "m"}, _FakeContext())
    assert out.attrs["embedding_stats"]["embedding_tokens"] is None


def test_do_embed_raw_sentence_transformers_reports_tokens_as_none(mod):
    """No token-billing concept for a local model -- must report None, not a guess."""
    import dagster as _dg  # noqa: F401
    df = pd.DataFrame({"chunk_text": ["hello world"]})
    _pytest_st = pytest.importorskip("sentence_transformers")
    out = mod._do_embed_raw(df, {"provider": "sentence_transformers", "model": "all-MiniLM-L6-v2"}, _FakeContext())
    stats = out.attrs["embedding_stats"]
    assert stats["embedding_tokens"] is None
    assert stats["embedding_model"] == "all-MiniLM-L6-v2"


def test_do_embed_reuse_from_path_with_nothing_to_compute_reports_zero_stats(mod, tmp_path):
    """The reuse_from branch when every chunk is already indexed (todo is empty) never calls
    _do_embed_raw at all -- must still report well-typed (not missing) embedding_stats."""
    pytest.importorskip("chromadb")
    import chromadb
    client = chromadb.PersistentClient(path=str(tmp_path / "reuse_chroma"))
    collection = client.get_or_create_collection(name="test_kb_collection")
    collection.upsert(
        ids=["d_0"], embeddings=[[0.1, 0.2]],
        metadatas=[{"content_hash": mod._sha256("hello"), "embedding_model": "m"}],
    )
    df = pd.DataFrame({"chunk_id": ["d_0"], "content_hash": [mod._sha256("hello")], "chunk_text": ["hello"]})
    out = mod._do_embed(
        df,
        {"provider": "litellm", "model": "m", "reuse_from": {"connection_string": str(tmp_path / "reuse_chroma"), "collection_name": "test_kb_collection"}},
        _FakeContext(),
    )
    assert out.attrs["stats"]["embeddings_computed"] == 0
    assert out.attrs["stats"]["embedding_tokens"] == 0
    assert out.attrs["stats"]["embedding_time_seconds"] == 0.0
