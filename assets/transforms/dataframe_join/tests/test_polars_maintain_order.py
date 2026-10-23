"""Real, committed test for a Polars 2.0 gap: polars' streaming query engine
(default since 2.0) doesn't guarantee join output row order unless
`maintain_order` is passed -- a silent reordering, not an error, so this
would never have shown up as a test failure unless the test explicitly
asserts on row order (which this one does). Confirmed against a real,
installed polars 2.0.0 -- not assumed.

Materializes through the real asset graph (dg.materialize +
dg.InMemoryIOManager), matching this component's existing
test_backfill_policy.py convention -- the join logic lives entirely inside
build_defs's compute closure, not a separately-callable function.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_join(mod, left_df, right_df, **overrides):
    @dg.asset(name="left_src")
    def left_src():
        return left_df

    @dg.asset(name="right_src")
    def right_src():
        return right_df

    kw = dict(
        asset_name="out_asset", left_asset_key="left_src", right_asset_key="right_src",
        how="inner", on=["id"], backend="polars",
    )
    kw.update(overrides)
    comp = mod.DataframeJoin(**kw)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [left_src, right_src] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node("out_asset")


def test_polars_backend_preserves_left_row_order_on_inner_join(mod):
    # Deliberately shuffled relative to `id` order, so a row-order bug would
    # actually change the assertion instead of coincidentally matching it.
    left = pd.DataFrame({"id": [3, 1, 2], "left_val": ["c", "a", "b"]})
    right = pd.DataFrame({"id": [1, 2, 3], "right_val": ["p", "q", "r"]})

    merged = _materialize_join(mod, left, right)
    assert merged["id"].to_list() == [3, 1, 2]
    assert merged["left_val"].to_list() == ["c", "a", "b"]


def test_polars_backend_preserves_right_row_order_on_right_join(mod):
    left = pd.DataFrame({"id": [1, 2, 3], "left_val": ["p", "q", "r"]})
    right = pd.DataFrame({"id": [3, 1, 2], "right_val": ["c", "a", "b"]})

    merged = _materialize_join(mod, left, right, how="right")
    assert merged["id"].to_list() == [3, 1, 2]
    assert merged["right_val"].to_list() == ["c", "a", "b"]
