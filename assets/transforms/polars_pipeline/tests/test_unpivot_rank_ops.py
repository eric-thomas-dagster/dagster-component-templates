"""Tests for the `unpivot` and `rank` ops added to polars_pipeline for ADF Data Flow
transform parity -- closes the same gap as pandas_pipeline/warehouse_pipeline's own
`unpivot`/`rank` additions this session (pivot/lookup/filter/unpivot/rank is a real ADF
Data Flow a user tried to replicate; unpivot/rank had no dedicated op before this).
"""
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_steps(mod, steps, asset_name="out_asset", primary_step=None):
    comp = mod.PolarsPipelineComponent(
        asset_name=asset_name,
        steps=steps,
        primary_step=primary_step,
    )
    result = dg.materialize(
        comp.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node(asset_name)


# -- unpivot ------------------------------------------------------------------

def test_unpivot_basic_reshape(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"id": 1, "q1": 10, "q2": 1},
                {"id": 2, "q1": 20, "q2": 2},
            ]}, "operations": [{
                "op": "unpivot", "id_columns": ["id"], "value_columns": ["q1", "q2"],
                "var_name": "quarter", "value_name": "amount",
            }]},
        ],
        asset_name="unpivoted",
    )
    assert set(out.columns) == {"id", "quarter", "amount"}
    assert len(out) == 4
    row = out.filter((out["id"] == 1) & (out["quarter"] == "q2"))
    assert row["amount"].to_list() == [1]


def test_unpivot_drop_null_values(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"id": 1, "a": 10, "b": None},
                {"id": 2, "a": None, "b": 20},
            ]}, "operations": [{
                "op": "unpivot", "id_columns": ["id"], "value_columns": ["a", "b"],
                "drop_null_values": True,
            }]},
        ],
        asset_name="unpivoted_dropna",
    )
    assert len(out) == 2
    assert out["value"].null_count() == 0


# -- rank -----------------------------------------------------------------------

def test_rank_single_column_dense(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"v": 10}, {"v": 20}, {"v": 20}, {"v": 30},
            ]}, "operations": [{"op": "rank", "order_by": "v", "method": "dense"}]},
        ],
        asset_name="ranked_dense",
    )
    out = out.sort("v")
    assert out["v_rank"].to_list() == [1.0, 2.0, 2.0, 3.0]


def test_rank_group_by(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"g": "a", "v": 10}, {"g": "a", "v": 20},
                {"g": "b", "v": 5}, {"g": "b", "v": 5},
            ]}, "operations": [{
                "op": "rank", "order_by": "v", "method": "dense", "group_by": ["g"],
            }]},
        ],
        asset_name="ranked_grouped",
    )
    a_ranks = sorted(out.filter(out["g"] == "a")["v_rank"].to_list())
    b_ranks = sorted(out.filter(out["g"] == "b")["v_rank"].to_list())
    assert a_ranks == [1.0, 2.0]
    assert b_ranks == [1.0, 1.0]


def test_rank_multi_column_order_by_mixed_direction(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"a": 1, "b": 5}, {"a": 1, "b": 3}, {"a": 2, "b": 1}, {"a": 2, "b": 1},
            ]}, "operations": [{
                "op": "rank", "order_by": ["a", "b"], "ascending": [True, False], "method": "dense",
            }]},
        ],
        asset_name="ranked_multi",
    )
    out = out.sort(["a", "b"], descending=[False, True])
    assert out["a_b_rank"].to_list() == [1.0, 2.0, 3.0, 3.0]


def test_rank_ntile(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"v": 5}, {"v": 5}, {"v": 10}, {"v": 20}, {"v": 20},
            ]}, "operations": [{
                "op": "rank", "order_by": "v", "method": "ntile", "ntile_buckets": 2,
            }]},
        ],
        asset_name="ranked_ntile",
    )
    out = out.sort("v")
    assert out["v_rank"].to_list() == [1, 1, 1, 2, 2]


def test_rank_cume_dist(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"v": 10}, {"v": 20}, {"v": 20}, {"v": 30},
            ]}, "operations": [{"op": "rank", "order_by": "v", "method": "cume_dist"}]},
        ],
        asset_name="ranked_cume_dist",
    )
    out = out.sort("v")
    assert out["v_rank"].to_list() == [0.25, 0.75, 0.75, 1.0]


def test_rank_invalid_method_raises(mod):
    with pytest.raises(ValueError, match="method"):
        _materialize_steps(
            mod,
            steps=[
                {"id": "s", "source": {"kind": "inline", "rows": [{"v": 1}]},
                 "operations": [{"op": "rank", "order_by": "v", "method": "bogus"}]},
            ],
            asset_name="ranked_bad",
        )
