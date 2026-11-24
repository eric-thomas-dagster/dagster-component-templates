"""Vector-metadata contract, writer side: document_chunker/text_chunker's stamp_chunk_contract (and
context_engineering_pipeline's stamp_provenance) stamp document_id/source_uri/page_start/page_end/
section/chunk_index/chunk_id/content_hash/extractor_version onto every chunk upstream -- this
component must carry those columns into every vector's own metadata automatically (without the
caller having to repeat each name in metadata_columns), and must complete the contract with
embedding_model, the one field a chunker running before embedding can't stamp itself.

Covers 3 real things fixed alongside this work:
1. Contract columns are auto-included in vector metadata (same mechanism as the existing
   `enabled_column` auto-include), tested against the real s3vectors demo-mode seam.
2. Null contract fields (page_start/page_end/source_uri/section default to null when their source
   column is unset) are dropped from metadata rather than written as None -- tested against REAL
   chromadb (no client mocking), since some vector stores reject non-str/int/float/bool values.
3. embedding_model is stamped per-vector AND promoted to this asset's own run metadata -- the exact
   key vector_store_query's knowledge_asset_key / `_infer_embed_model_from_asset` already expected
   this component to write, but it never actually did until now.
4. id_column, left unset, falls back to an upstream-stamped `chunk_id` column for idempotent vector
   ids with nothing to configure.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb

mod = load_component_module()
VectorStoreWriterComponent = mod.VectorStoreWriterComponent


def _component(**kw):
    attrs = dict(
        asset_name="vecs", provider="s3vectors", collection_name="idx",
        vector_bucket_name="buck", aws_region="us-east-2", upstream_asset_key="embeddings",
    )
    attrs.update(kw)
    return VectorStoreWriterComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="embeddings")
    def embeddings():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([embeddings, *defs.assets], **kwargs)


def _demo_metadatas(demo_path):
    data = json.loads(demo_path.read_text())
    vectors = data["indexes"]["idx"]["vectors"]
    return {key: v["metadata"] for key, v in vectors.items()}


def _contract_df():
    return pd.DataFrame({
        "embedding": [[0.1, 0.2], [0.3, 0.4]],
        "chunk_text": ["a", "b"],
        "chunk_id": ["doc-a_0", "doc-a_1"],
        "document_id": ["doc-a", "doc-a"],
        "source_uri": ["s3://x/a.md", "s3://x/a.md"],
        "page_start": [None, None],
        "page_end": [None, None],
        "section": [None, "Details"],
        "chunk_index": [0, 1],
        "content_hash": ["h0", "h1"],
        "extractor_version": ["cfg-1", "cfg-1"],
    })


def test_contract_columns_auto_included_in_vector_metadata(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), text_column="chunk_text")
    result = _materialize(comp, _contract_df(), raise_on_error=True)
    assert result.success
    metas = _demo_metadatas(demo_path)
    # ids come from the fallback to 'chunk_id' (id_column left unset)
    assert set(metas.keys()) == {"doc-a_0", "doc-a_1"}
    assert metas["doc-a_0"]["document_id"] == "doc-a"
    assert metas["doc-a_0"]["content_hash"] == "h0"
    assert metas["doc-a_0"]["extractor_version"] == "cfg-1"
    # null fields (page_start/page_end/source_uri*/section) are dropped, not written as None
    assert "page_start" not in metas["doc-a_0"]
    assert "page_end" not in metas["doc-a_0"]
    assert "section" not in metas["doc-a_0"]
    assert metas["doc-a_1"]["section"] == "Details"


def test_id_column_falls_back_to_upstream_chunk_id(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), text_column="chunk_text")
    _materialize(comp, _contract_df(), raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert "doc-a_0" in metas and "doc-a_1" in metas
    assert "doc_0" not in metas  # NOT the old positional fallback


def test_explicit_id_column_still_wins_over_chunk_id_fallback(tmp_path):
    demo_path = tmp_path / "demo.json"
    df = _contract_df()
    df["my_id"] = ["custom-0", "custom-1"]
    comp = _component(connection_string=str(demo_path), text_column="chunk_text", id_column="my_id")
    _materialize(comp, df, raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert set(metas.keys()) == {"custom-0", "custom-1"}


def test_embedding_model_stamped_per_vector_and_promoted_to_run_metadata(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(
        connection_string=str(demo_path), text_column="chunk_text", embedding_model="all-MiniLM-L6-v2",
    )
    result = _materialize(comp, _contract_df(), raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert metas["doc-a_0"]["embedding_model"] == "all-MiniLM-L6-v2"
    assert metas["doc-a_1"]["embedding_model"] == "all-MiniLM-L6-v2"
    run_meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert run_meta["embedding_model"].value == "all-MiniLM-L6-v2"


def test_embedding_model_column_overrides_static_default_per_row(tmp_path):
    demo_path = tmp_path / "demo.json"
    df = _contract_df()
    df["model_used"] = ["model-a", "model-b"]
    comp = _component(
        connection_string=str(demo_path), text_column="chunk_text",
        embedding_model="fallback-model", embedding_model_column="model_used",
    )
    _materialize(comp, df, raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert metas["doc-a_0"]["embedding_model"] == "model-a"
    assert metas["doc-a_1"]["embedding_model"] == "model-b"


def test_no_embedding_model_configured_omits_it_entirely(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), text_column="chunk_text")
    result = _materialize(comp, _contract_df(), raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert "embedding_model" not in metas["doc-a_0"]
    run_meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "embedding_model" not in run_meta


@requires_chromadb
def test_null_contract_fields_do_not_break_a_real_chromadb_write(tmp_path):
    """Real chromadb (no client mocking) -- confirms the None-stripping is harmless when a
    provider silently drops None values, and still verifies the real metadata that landed."""
    comp = _component(
        provider="chromadb", connection_string=str(tmp_path / "chroma_db"),
        collection_name="contract_test", text_column="chunk_text",
    )
    result = _materialize(comp, _contract_df(), raise_on_error=True)
    assert result.success

    import chromadb
    client = chromadb.PersistentClient(path=str(tmp_path / "chroma_db"))
    collection = client.get_collection("contract_test")
    got = collection.get(ids=["doc-a_0"], include=["metadatas"])
    meta = got["metadatas"][0]
    assert meta["document_id"] == "doc-a"
    assert "page_start" not in meta
    assert "page_end" not in meta
