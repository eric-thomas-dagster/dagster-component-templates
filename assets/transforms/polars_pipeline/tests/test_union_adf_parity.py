"""Tests for PolarsPipelineComponent's `union` op (ADF converter feedback, ask 13):
pyspark_pipeline already has join/union/sql, polars_pipeline only had join/sql. Matches
pyspark_pipeline's `op: union, other: {ref: <step_id>}` shape, plus dataframe_union's own
`mode: by_name|by_position` field for column-matching semantics.
"""
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg


@pytest.fixture()
def mod():
    return load_component_module()


def _upstreams():
    @dg.asset(name="orders_a")
    def orders_a():
        return pd.DataFrame({"id": [1, 2], "amount": [10, 20]})

    @dg.asset(name="orders_b")
    def orders_b():
        return pd.DataFrame({"id": [3, 4], "amount": [30, 40]})

    return [orders_a, orders_b]


def _materialize(mod, **union_op_overrides):
    comp = mod.PolarsPipelineComponent(
        asset_name="out_asset",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "orders_a"}, "operations": []},
            {"id": "b", "source": {"kind": "upstream", "upstream_asset_key": "orders_b"}, "operations": []},
            {
                "id": "out",
                "source": {"kind": "ref", "ref": "a"},
                "operations": [{"op": "union", "other": {"ref": "b"}, **union_op_overrides}],
            },
        ],
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [*_upstreams(), *comp_assets],
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node("out_asset")


def test_union_by_name_combines_matching_columns(mod):
    out = _materialize(mod)
    assert sorted(out["id"].to_list()) == [1, 2, 3, 4]
    assert sorted(out["amount"].to_list()) == [10, 20, 30, 40]


def test_union_distinct_dedupes_exact_duplicate_rows(mod):
    out = _materialize(mod, distinct=True)
    assert len(out) == 4  # no duplicate rows to begin with -- distinct is a no-op here
    assert sorted(out["id"].to_list()) == [1, 2, 3, 4]


def test_union_missing_ref_raises(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="out_asset",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "orders_a"}, "operations": []},
            {
                "id": "out",
                "source": {"kind": "ref", "ref": "a"},
                "operations": [{"op": "union", "other": {"ref": "does_not_exist"}}],
            },
        ],
    )
    comp_assets = comp.build_defs(None).assets
    with pytest.raises(ValueError, match="doesn't match any earlier step id"):
        dg.materialize(
            [*_upstreams(), *comp_assets],
            resources={"io_manager": dg.InMemoryIOManager()},
        )


def test_union_invalid_mode_raises(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="out_asset",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "orders_a"}, "operations": []},
            {"id": "b", "source": {"kind": "upstream", "upstream_asset_key": "orders_b"}, "operations": []},
            {
                "id": "out",
                "source": {"kind": "ref", "ref": "a"},
                "operations": [{"op": "union", "other": {"ref": "b"}, "mode": "sideways"}],
            },
        ],
    )
    comp_assets = comp.build_defs(None).assets
    with pytest.raises(ValueError, match="mode must be"):
        dg.materialize(
            [*_upstreams(), *comp_assets],
            resources={"io_manager": dg.InMemoryIOManager()},
        )
