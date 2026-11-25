"""Committed tests for ArrayExploderComponent's move_exploded_to_end option.

StreamSets' List Pivot always moves the pivoted field to the end of the
record. pandas' own DataFrame.explode() leaves the exploded column in its
original position. Real dg.materialize() runs.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, **overrides):
    @dg.asset(name="src")
    def src():
        return pd.DataFrame({"id": [1, 2], "items": [["a", "b"], ["c"]], "trailer": ["x", "y"]})

    cls = getattr(mod, "ArrayExploderComponent")
    comp = cls(asset_name="out_asset", upstream_asset_key="src", column="items", **overrides)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [src] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    out = result.output_for_node("out_asset")
    return out.value if hasattr(out, "value") and hasattr(out, "metadata") else out


def test_default_keeps_original_column_order(mod):
    out = _materialize(mod)
    assert list(out.columns) == ["id", "items", "trailer"]


def test_move_exploded_to_end_reorders(mod):
    out = _materialize(mod, move_exploded_to_end=True)
    assert list(out.columns) == ["id", "trailer", "items"]
    # Row-level explode semantics are unaffected by the reorder.
    assert out["items"].tolist() == ["a", "b", "c"]
