"""Committed tests for CrossTabComponent's ADF-Pivot-parity additions:
multi-column index_column, multi-column value_column (+ per-column agg_func
dict), and pivot_values (restricting + guaranteeing columns for values absent
from the data). Real pandas frames, real dg.materialize(), nothing mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

CLS = "CrossTabComponent"


@pytest.fixture()
def mod():
    return load_component_module()


def _sales_df():
    return pd.DataFrame(
        [
            {"region": "east", "segment": "retail", "quarter": "Q1", "revenue": 100.0, "units": 10.0},
            {"region": "east", "segment": "retail", "quarter": "Q2", "revenue": 150.0, "units": 12.0},
            {"region": "east", "segment": "wholesale", "quarter": "Q1", "revenue": 200.0, "units": 20.0},
            {"region": "west", "segment": "retail", "quarter": "Q1", "revenue": 300.0, "units": 30.0},
            {"region": "west", "segment": "retail", "quarter": "Q3", "revenue": 50.0, "units": 5.0},
        ]
    )


def _materialize(mod, df, **kwargs):
    @dg.asset(name="sales")
    def sales():
        return df

    cls = getattr(mod, CLS)
    comp = cls(asset_name="out_asset", upstream_asset_key="sales", **kwargs)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [sales] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    out = result.output_for_node("out_asset")
    return out.value if hasattr(out, "value") and hasattr(out, "metadata") else out


def test_single_column_default_behavior_unchanged(mod):
    """Pre-existing scenario (exact shape test_backfill_policy.py's _build()
    uses for the unpartitioned case): single index/pivot/value columns, no
    pivot_values. Must still produce the same pivot output as before this
    change -- no extra/renamed/reordered columns."""
    df = _sales_df()
    out = _materialize(
        mod, df,
        index_column="region", pivot_column="quarter", value_column="revenue",
        agg_func="sum", fill_value=0,
    )
    expected = pd.pivot_table(
        df, index="region", columns="quarter", values="revenue",
        aggfunc="sum", fill_value=0,
    ).reset_index()
    expected.columns = [str(c) if not isinstance(c, str) else c for c in expected.columns]
    pd.testing.assert_frame_equal(
        out.reset_index(drop=True), expected.reset_index(drop=True), check_like=False
    )


def test_multi_column_index(mod):
    df = _sales_df()
    out = _materialize(
        mod, df,
        index_column=["region", "segment"], pivot_column="quarter", value_column="revenue",
        agg_func="sum", fill_value=0,
    )
    assert "region" in out.columns and "segment" in out.columns
    # (east, retail) row: Q1=100, Q2=150, Q3=0 (fill_value)
    row = out[(out["region"] == "east") & (out["segment"] == "retail")].iloc[0]
    assert row["Q1"] == 100.0
    assert row["Q2"] == 150.0
    assert row["Q3"] == 0.0
    # (west, retail) row: Q1=300, Q3=50
    row2 = out[(out["region"] == "west") & (out["segment"] == "retail")].iloc[0]
    assert row2["Q1"] == 300.0
    assert row2["Q3"] == 50.0


def test_multi_column_value_with_per_column_agg_func(mod):
    df = _sales_df()
    out = _materialize(
        mod, df,
        index_column="region", pivot_column="quarter", value_column=["revenue", "units"],
        agg_func={"revenue": "sum", "units": "mean"}, fill_value=0,
    )
    # Expect one set of pivoted columns per value column: revenue_Q1, units_Q1, etc.
    assert "revenue_Q1" in out.columns
    assert "units_Q1" in out.columns
    east = out[out["region"] == "east"].iloc[0]
    # revenue summed: Q1 = 100 (retail) + 200 (wholesale) = 300
    assert east["revenue_Q1"] == 300.0
    # units mean: Q1 = mean(10, 20) = 15
    assert east["units_Q1"] == 15.0


def test_pivot_values_restricts_and_guarantees_fill_column(mod):
    """pivot_values=['Q1', 'Q4'] -- Q4 never appears in the data and must
    still produce an all-fill_value column; Q2/Q3 (present in the data but
    not listed) must be excluded entirely."""
    df = _sales_df()
    out = _materialize(
        mod, df,
        index_column="region", pivot_column="quarter", value_column="revenue",
        agg_func="sum", fill_value=0, pivot_values=["Q1", "Q4"],
    )
    assert "Q1" in out.columns
    assert "Q4" in out.columns
    assert "Q2" not in out.columns
    assert "Q3" not in out.columns
    # Q4 never appears in the data -> entirely fill_value
    assert (out["Q4"] == 0).all()
    east = out[out["region"] == "east"].iloc[0]
    assert east["Q1"] == 300.0  # 100 (retail) + 200 (wholesale)


def test_pivot_values_unset_keeps_every_unique_value(mod):
    df = _sales_df()
    out = _materialize(
        mod, df,
        index_column="region", pivot_column="quarter", value_column="revenue",
        agg_func="sum", fill_value=0,
    )
    assert set(out.columns) == {"region", "Q1", "Q2", "Q3"}
