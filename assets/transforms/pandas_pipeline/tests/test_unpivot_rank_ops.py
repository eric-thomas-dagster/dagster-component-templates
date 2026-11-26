"""Tests for the `unpivot` and `rank` ops added to pandas_pipeline for ADF Data Flow
transform parity: a user asked a different agent to replicate an ADF data flow (pivot ->
lookup -> filter -> unpivot -> rank) via pandas_pipeline and was correctly told unpivot/rank
had no dedicated op -- this closes that gap, same session/pattern as the already-closed
pivot/lookup/window_calculation gaps.

Every test actually materializes the asset via dagster.materialize(), same convention as
test_adf_gap_ops.py.
"""
import dagster as dg
import pandas as pd
import pytest


def _materialize_flat(mod, upstream_df, operations, **extra):
    @dg.asset
    def src():
        return upstream_df

    comp = mod.PandasPipelineComponent(
        asset_name="out_asset",
        upstream_asset_key="src",
        operations=operations,
        **extra,
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[src]), comp.build_defs(None))
    result = dg.materialize([src, defs.get_assets_def(dg.AssetKey("out_asset"))])
    return result.output_for_node("out_asset")


# -- unpivot ------------------------------------------------------------------

def test_unpivot_basic_reshape(mod):
    df = pd.DataFrame({"id": [1, 2], "q1": [10, 20], "q2": [1, 2]})
    out = _materialize_flat(mod, df, [{
        "op": "unpivot", "id_columns": ["id"], "value_columns": ["q1", "q2"],
        "var_name": "quarter", "value_name": "amount",
    }])
    assert list(out.columns) == ["id", "quarter", "amount"]
    assert len(out) == 4
    row = out[(out["id"] == 1) & (out["quarter"] == "q2")].iloc[0]
    assert row["amount"] == 1


def test_unpivot_defaults_to_all_non_id_columns(mod):
    df = pd.DataFrame({"id": [1], "a": [10], "b": [20]})
    out = _materialize_flat(mod, df, [{"op": "unpivot", "id_columns": ["id"]}])
    assert set(out["variable"]) == {"a", "b"}


def test_unpivot_drop_null_values(mod):
    df = pd.DataFrame({"id": [1, 2], "a": [10, None], "b": [None, 20]})
    out = _materialize_flat(mod, df, [{
        "op": "unpivot", "id_columns": ["id"], "value_columns": ["a", "b"],
        "drop_null_values": True,
    }])
    assert len(out) == 2
    assert not out["value"].isna().any()


def test_unpivot_requires_id_columns(mod):
    df = pd.DataFrame({"id": [1], "a": [10]})
    with pytest.raises(ValueError, match="id_columns"):
        _materialize_flat(mod, df, [{"op": "unpivot"}])


# -- rank: tie-breaking methods -------------------------------------------------

def test_rank_dense_method(mod):
    df = pd.DataFrame({"v": [10, 20, 20, 30]})
    out = _materialize_flat(mod, df, [{"op": "rank", "order_by": "v", "method": "dense"}])
    assert out.sort_values("v")["v_rank"].tolist() == [1.0, 2.0, 2.0, 3.0]


def test_rank_min_method_ties_get_min_position(mod):
    df = pd.DataFrame({"v": [10, 20, 20, 30]})
    out = _materialize_flat(mod, df, [{"op": "rank", "order_by": "v", "method": "min"}])
    assert out.sort_values("v")["v_rank"].tolist() == [1.0, 2.0, 2.0, 4.0]


def test_rank_group_by(mod):
    df = pd.DataFrame({"g": ["a", "a", "b", "b"], "v": [10, 20, 5, 5]})
    out = _materialize_flat(mod, df, [{
        "op": "rank", "order_by": "v", "method": "dense", "group_by": ["g"],
    }])
    a_ranks = sorted(out[out["g"] == "a"]["v_rank"].tolist())
    b_ranks = sorted(out[out["g"] == "b"]["v_rank"].tolist())
    assert a_ranks == [1.0, 2.0]
    assert b_ranks == [1.0, 1.0]


def test_rank_ntile(mod):
    df = pd.DataFrame({"v": [5, 5, 10, 20, 20]})
    out = _materialize_flat(mod, df, [{
        "op": "rank", "order_by": "v", "method": "ntile", "ntile_buckets": 2,
    }])
    # 5 rows / 2 buckets -> sizes [3, 2] (first bucket gets the extra row), ordered by v asc.
    assert out.sort_values("v")["v_rank"].tolist() == [1.0, 1.0, 1.0, 2.0, 2.0]


def test_rank_cume_dist(mod):
    df = pd.DataFrame({"v": [10, 20, 20, 30]})
    out = _materialize_flat(mod, df, [{"op": "rank", "order_by": "v", "method": "cume_dist"}])
    assert out.sort_values("v")["v_rank"].tolist() == [0.25, 0.75, 0.75, 1.0]


def test_rank_ntile_requires_bucket_count(mod):
    df = pd.DataFrame({"v": [1, 2]})
    with pytest.raises(ValueError, match="ntile_buckets"):
        _materialize_flat(mod, df, [{"op": "rank", "order_by": "v", "method": "ntile"}])


def test_rank_invalid_method_raises(mod):
    df = pd.DataFrame({"v": [1, 2]})
    with pytest.raises(ValueError, match="method"):
        _materialize_flat(mod, df, [{"op": "rank", "order_by": "v", "method": "bogus"}])
