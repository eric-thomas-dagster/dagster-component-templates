"""Run metadata: queries_count, hits_count, avg/min/max_distance, sample_hits -- without these a
retrieval-testing UI has nothing to read after a query run (the feedback this was built from).
Exercised for real against the s3vectors demo-mode seam (brute-force cosine ranking over a local
JSON file, no AWS calls), matching this directory's sibling test_s3vectors_provider.py.
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


def test_metadata_reports_queries_hits_and_distance_stats(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        ("doc_0", [1.0, 0.0], {"text": "cat"}),
        ("doc_1", [0.0, 1.0], {"text": "dog"}),
        ("doc_2", [0.9, 0.1], {"text": "kitten"}),
    ])
    comp = _component(connection_string=str(demo_path), top_k=2)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["queries_count"].value == 1
    assert meta["hits_count"].value == 2
    assert meta["min_distance"].value <= meta["avg_distance"].value <= meta["max_distance"].value


def test_sample_hits_includes_real_hit_rows(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "cat"})])
    comp = _component(connection_string=str(demo_path), top_k=1)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "doc_0" in meta["sample_hits"].value


def test_distance_stats_absent_when_include_distances_false(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [("doc_0", [1.0, 0.0], {"text": "cat"})])
    comp = _component(connection_string=str(demo_path), top_k=1, include_distances=False)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "avg_distance" not in meta
    assert meta["hits_count"].value == 1  # the non-distance metadata still reports
