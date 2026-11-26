"""Real tests for ADF-"Exists"-style semi/anti join support.

Gap: pandas' own `.merge()` has no native semi/anti concept at all, unlike
polars' `.join(how=...)`, which already passed 'semi'/'anti' straight
through to a real polars join. With the DEFAULT backend ('pandas'),
`how='semi'`/`how='anti'` either crashed or silently misbehaved before this
fix. These tests exercise the pandas path explicitly (not relying on the
default, so the gap stays covered even if the default ever changes) plus a
polars-backend test confirming that existing path is untouched.

Materializes through the real asset graph (dg.materialize +
dg.InMemoryIOManager), matching this component's existing
test_backfill_policy.py / test_polars_maintain_order.py convention -- the
join logic lives entirely inside build_defs's compute closure, not a
separately-callable function.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


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
        on=["id"],
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


def test_pandas_semi_join_keeps_only_matched_left_rows_no_right_columns(mod):
    left = pd.DataFrame({"id": [1, 2, 3], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"id": [2, 3, 3], "right_val": ["x", "y", "z"]})

    merged = _materialize_join(mod, left, right, how="semi", backend="pandas")

    assert list(merged["id"]) == [2, 3]
    assert list(merged["left_val"]) == ["b", "c"]
    assert "right_val" not in merged.columns
    # Right side had a duplicate match for id=3 -- semi join must not duplicate rows.
    assert len(merged) == 2


def test_pandas_anti_join_keeps_only_unmatched_left_rows(mod):
    left = pd.DataFrame({"id": [1, 2, 3], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"id": [2, 3, 3], "right_val": ["x", "y", "z"]})

    merged = _materialize_join(mod, left, right, how="anti", backend="pandas")

    assert list(merged["id"]) == [1]
    assert list(merged["left_val"]) == ["a"]
    assert "right_val" not in merged.columns


def test_pandas_semi_join_with_left_on_right_on_differing_key_names(mod):
    left = pd.DataFrame({"lid": [1, 2, 3], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"rid": [1, 3], "right_val": ["x", "y"]})

    merged = _materialize_join(
        mod, left, right, how="semi", backend="pandas",
        on=None, left_on=["lid"], right_on=["rid"],
    )

    assert list(merged["lid"]) == [1, 3]
    assert "right_val" not in merged.columns
    assert "rid" not in merged.columns


def test_pandas_anti_join_with_left_on_right_on_differing_key_names(mod):
    left = pd.DataFrame({"lid": [1, 2, 3], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"rid": [1, 3], "right_val": ["x", "y"]})

    merged = _materialize_join(
        mod, left, right, how="anti", backend="pandas",
        on=None, left_on=["lid"], right_on=["rid"],
    )

    assert list(merged["lid"]) == [2]


def test_polars_semi_anti_join_still_works_unchanged(mod):
    pytest.importorskip("polars")
    left = pd.DataFrame({"id": [1, 2, 3], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"id": [2, 3, 3], "right_val": ["x", "y", "z"]})

    semi = _materialize_join(mod, left, right, how="semi", backend="polars")
    anti = _materialize_join(mod, left, right, how="anti", backend="polars")

    assert sorted(semi["id"].to_list()) == [2, 3]
    assert "right_val" not in semi.columns
    assert anti["id"].to_list() == [1]
    assert "right_val" not in anti.columns
