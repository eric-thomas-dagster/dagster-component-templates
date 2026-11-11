"""Run metadata: chunk_count, store_type, embedding_dimension, sample_chunks -- without these a
UI showing "what got written" has nothing to read (the feedback this was built from). Exercised
for real against the s3vectors demo-mode seam (no AWS calls, matching this file's sibling
test_s3vectors_provider.py).
"""
import dagster as dg
import pandas as pd

from .conftest import load_component_module

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


def test_metadata_reports_chunk_count_store_type_and_embedding_dimension(tmp_path):
    comp = _component(connection_string=str(tmp_path / "demo.json"))
    df = pd.DataFrame({"embedding": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]], "text": ["a", "b"]})
    result = _materialize(comp, df, raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["chunk_count"].value == 2
    assert meta["store_type"].value == "s3vectors"
    assert meta["embedding_dimension"].value == 3


def test_sample_chunks_excludes_raw_embedding_vector(tmp_path):
    comp = _component(connection_string=str(tmp_path / "demo.json"))
    df = pd.DataFrame({"embedding": [[0.1, 0.2, 0.3]], "text": ["hello"]})
    result = _materialize(comp, df, raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    sample_md = meta["sample_chunks"].value
    assert "hello" in sample_md
    assert "0.1" not in sample_md  # the raw embedding vector is unreadable noise, not a useful sample


def test_preview_can_be_disabled(tmp_path):
    comp = _component(connection_string=str(tmp_path / "demo.json"), include_preview_metadata=False)
    df = pd.DataFrame({"embedding": [[0.1, 0.2, 0.3]], "text": ["hello"]})
    result = _materialize(comp, df, raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "sample_chunks" not in meta
    assert meta["chunk_count"].value == 1  # the non-preview metadata still reports
