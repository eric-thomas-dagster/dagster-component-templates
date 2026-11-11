"""exclude_disabled (drops hits whose 'enabled' metadata is explicitly false -- the read side of
vector_store_writer's enabled_column) and track_retrieval_count (opt-in write-back of a
'retrieval_count' metadata field on every returned hit) -- exercised against the real s3vectors
demo-mode seam (no AWS calls), matching this directory's sibling test_s3vectors_provider.py.
"""
import json

import dagster as dg
import pandas as pd

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent


def _write_demo_file(path, rows):
    data = {"indexes": {"idx": {"dimension": len(rows[0][1]), "distanceMetric": "cosine", "vectors": {}}}}
    for key, vector, metadata in rows:
        data["indexes"]["idx"]["vectors"][key] = {"data": {"float32": vector}, "metadata": metadata}
    path.write_text(json.dumps(data))


def _read_demo_metadatas(path):
    data = json.loads(path.read_text())
    return {key: v["metadata"] for key, v in data["indexes"]["idx"]["vectors"].items()}


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


def test_exclude_disabled_drops_hits_marked_enabled_false(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        ("doc_0", [1.0, 0.0], {"text": "a", "enabled": True}),
        ("doc_1", [1.0, 0.0], {"text": "b", "enabled": False}),
    ])
    comp = _component(connection_string=str(demo_path), top_k=2, exclude_disabled=True)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    assert result.success
    out = result.asset_value("results")
    assert len(out) == 1
    assert out.iloc[0]["key"] == "doc_0"


def test_exclude_disabled_keeps_hits_missing_the_enabled_key(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "a"})])
    comp = _component(connection_string=str(demo_path), top_k=2, exclude_disabled=True)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    out = result.asset_value("results")
    assert len(out) == 1


def test_exclude_disabled_false_keeps_disabled_hits(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        ("doc_0", [1.0, 0.0], {"text": "a", "enabled": True}),
        ("doc_1", [1.0, 0.0], {"text": "b", "enabled": False}),
    ])
    comp = _component(connection_string=str(demo_path), top_k=2, exclude_disabled=False)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    out = result.asset_value("results")
    assert len(out) == 2


def test_track_retrieval_count_increments_hit_metadata(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "a"})])
    comp = _component(connection_string=str(demo_path), top_k=1, track_retrieval_count=True)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    assert result.success
    metas = _read_demo_metadatas(demo_path)
    assert metas["doc_0"]["retrieval_count"] == 1


def test_track_retrieval_count_accumulates_across_runs(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "a"})])
    comp = _component(connection_string=str(demo_path), top_k=1, track_retrieval_count=True)
    _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    metas = _read_demo_metadatas(demo_path)
    assert metas["doc_0"]["retrieval_count"] == 2


def test_track_retrieval_count_defaults_to_false_and_does_not_touch_the_file(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "a"})])
    comp = _component(connection_string=str(demo_path), top_k=1)
    _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    metas = _read_demo_metadatas(demo_path)
    assert "retrieval_count" not in metas["doc_0"]
