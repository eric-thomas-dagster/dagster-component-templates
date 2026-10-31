"""provider='s3vectors': real end-to-end tests of the demo-mode seam
(brute-force cosine ranking over the same local JSON file
VectorStoreWriterComponent's demo mode writes), filter_metadata pass-through,
and the real-mode boto3 client construction seam.

`dg.materialize()` is real throughout. The other 4 providers (chromadb,
pinecone, qdrant, faiss) are untouched by this change and are not re-tested
here.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent


def _write_demo_file(path, rows):
    """rows: list of (key, vector, metadata) -- mirrors VectorStoreWriterComponent's
    own demo-mode JSON shape exactly, so this test doesn't need that sibling
    component to build the fixture."""
    data = {"indexes": {"idx": {"dimension": len(rows[0][1]), "distanceMetric": "cosine", "vectors": {}}}}
    for key, vector, metadata in rows:
        data["indexes"]["idx"]["vectors"][key] = {"data": {"float32": vector}, "metadata": metadata}
    path.write_text(json.dumps(data))


def _component(**kw):
    attrs = dict(
        asset_name="results", provider="s3vectors", collection_name="idx",
        vector_bucket_name="buck", aws_region="us-east-2", upstream_asset_key="queries",
    )
    attrs.update(kw)
    return VectorStoreQueryComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="queries")
    def queries():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([queries, *defs.assets], **kwargs)


def test_demo_mode_ranks_by_cosine_similarity(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        ("doc_0", [1.0, 0.0], {"text": "cat"}),
        ("doc_1", [0.0, 1.0], {"text": "dog"}),
        ("doc_2", [0.9, 0.1], {"text": "kitten"}),
    ])
    comp = _component(connection_string=str(demo_path), top_k=2)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    df = result.asset_value("results")
    assert list(df["key"]) == ["doc_0", "doc_2"]
    assert df["distance"].iloc[0] < df["distance"].iloc[1]


def test_filter_metadata_restricts_results(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        ("doc_0", [1.0, 0.0], {"species": "feline"}),
        ("doc_1", [0.9, 0.1], {"species": "canine"}),
    ])
    comp = _component(connection_string=str(demo_path), top_k=5, filter_metadata='{"species": "canine"}')
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    df = result.asset_value("results")
    assert list(df["key"]) == ["doc_1"]


def test_empty_index_returns_no_results(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), top_k=5)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    assert len(result.asset_value("results")) == 0


def test_include_distances_false_omits_distance_column(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {})])
    comp = _component(connection_string=str(demo_path), top_k=5, include_distances=False)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    assert "distance" not in result.asset_value("results").columns


def test_real_mode_seam_passes_correct_query_shape():
    captured = {}

    class FakeClient:
        def query_vectors(self, **kw):
            captured.update(kw)
            return {"vectors": [{"key": "doc_0", "metadata": {}, "distance": 0.1}]}

    mod._build_s3vectors_client = lambda region: FakeClient()
    comp = _component(demo_mode=False, top_k=3)
    _materialize(comp, pd.DataFrame({"embedding": [[0.5, 0.5]]}), raise_on_error=True)
    assert captured["vectorBucketName"] == "buck"
    assert captured["indexName"] == "idx"
    assert captured["topK"] == 3
    assert captured["queryVector"] == {"float32": [0.5, 0.5]}


def test_missing_vector_bucket_name_raises():
    with pytest.raises(Exception, match="requires both"):
        VectorStoreQueryComponent(
            asset_name="results", provider="s3vectors", collection_name="idx",
            aws_region="us-east-2", upstream_asset_key="queries",
        )
