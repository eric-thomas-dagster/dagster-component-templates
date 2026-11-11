"""enabled_column (per-document, data-driven enable/disable written into vector metadata) and
override_enabled (a launchpad/API-settable run config field that freezes/unfreezes every vector
for a single run, regardless of enabled_column) -- exercised against the real s3vectors demo-mode
seam (no AWS calls), matching this directory's sibling test_run_metadata.py.
"""
import json

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


def _demo_metadatas(demo_path):
    data = json.loads(demo_path.read_text())
    vectors = data["indexes"]["idx"]["vectors"]
    return {key: v["metadata"] for key, v in vectors.items()}


def test_enabled_column_writes_enabled_true_false_per_row(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), enabled_column="is_enabled")
    df = pd.DataFrame({
        "embedding": [[0.1, 0.2], [0.3, 0.4]],
        "text": ["a", "b"],
        "is_enabled": [True, False],
    })
    result = _materialize(comp, df, raise_on_error=True)
    assert result.success
    metas = _demo_metadatas(demo_path)
    assert metas["doc_0"]["enabled"] is True
    assert metas["doc_1"]["enabled"] is False


def test_enabled_column_missing_or_nan_value_defaults_to_enabled(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), enabled_column="is_enabled")
    df = pd.DataFrame({
        "embedding": [[0.1, 0.2]],
        "text": ["a"],
        "is_enabled": [None],
    })
    result = _materialize(comp, df, raise_on_error=True)
    assert result.success
    metas = _demo_metadatas(demo_path)
    assert metas["doc_0"]["enabled"] is True


def test_enabled_column_not_in_dataframe_raises():
    comp = _component(enabled_column="is_enabled")
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(comp, df, raise_on_error=False)
    assert not result.success


def test_no_enabled_column_omits_enabled_metadata_entirely(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path))
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(comp, df, raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert "enabled" not in metas["doc_0"]


def test_override_enabled_run_config_freezes_everything_regardless_of_enabled_column(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path), enabled_column="is_enabled")
    df = pd.DataFrame({
        "embedding": [[0.1, 0.2], [0.3, 0.4]],
        "text": ["a", "b"],
        "is_enabled": [True, True],
    })
    result = _materialize(
        comp, df, raise_on_error=True,
        run_config={"ops": {"vecs": {"config": {"override_enabled": False}}}},
    )
    assert result.success
    metas = _demo_metadatas(demo_path)
    assert metas["doc_0"]["enabled"] is False
    assert metas["doc_1"]["enabled"] is False


def test_override_enabled_run_config_works_even_without_enabled_column(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path))
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(
        comp, df, raise_on_error=True,
        run_config={"ops": {"vecs": {"config": {"override_enabled": True}}}},
    )
    assert result.success
    metas = _demo_metadatas(demo_path)
    assert metas["doc_0"]["enabled"] is True


def test_unset_override_enabled_defaults_to_no_op(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path))
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(comp, df, raise_on_error=True)
    metas = _demo_metadatas(demo_path)
    assert "enabled" not in metas["doc_0"]
