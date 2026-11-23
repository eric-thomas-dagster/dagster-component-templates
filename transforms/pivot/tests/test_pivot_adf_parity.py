"""Tests for the ADF Mapping Data Flow parity additions: multi-column pivot
(`value_columns`), an explicit pivot-values allow-list (`pivot_values`), and
the `$V$N`-style `column_naming_pattern`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _upstream(df: pd.DataFrame):
    @dg.asset(name="upstream")
    def _upstream_asset():
        return df

    return _upstream_asset


def _materialize(mod, df, **kwargs):
    kwargs.setdefault("asset_name", "out_asset")
    kwargs["upstream_asset_key"] = "upstream"
    comp = mod.PivotComponent(**kwargs)
    defs = comp.build_defs(context=None)
    upstream = _upstream(df)
    result = dg.materialize([upstream, *defs.assets])
    assert result.success
    return result.asset_value("out_asset")


def _base_df():
    return pd.DataFrame(
        {
            "id": [1, 1, 2, 2],
            "sensor": ["a", "b", "a", "b"],
            "speed": [10, 20, 30, 40],
            "accel": [1, 2, 3, 4],
        }
    )


def test_single_value_pivot_unchanged(mod):
    out = _materialize(mod, _base_df(), index_columns=["id"], pivot_column="sensor", value_column="speed")
    assert sorted(out.columns) == ["a", "b", "id"]
    row = out[out["id"] == 1].iloc[0]
    assert row["a"] == 10 and row["b"] == 20


def test_column_prefix_still_works(mod):
    out = _materialize(
        mod, _base_df(), index_columns=["id"], pivot_column="sensor", value_column="speed", column_prefix="Speed_",
    )
    assert sorted(out.columns) == ["Speed_a", "Speed_b", "id"]


def test_multi_value_pivot_with_naming_pattern(mod):
    out = _materialize(
        mod,
        _base_df(),
        index_columns=["id"],
        pivot_column="sensor",
        value_columns=[
            {"column": "speed", "agg_func": "sum", "alias": "Speed"},
            {"column": "accel", "agg_func": "sum", "alias": "Accel"},
        ],
        column_naming_pattern="$V_$N",
    )
    assert sorted(out.columns) == sorted(["id", "Speed_a", "Speed_b", "Accel_a", "Accel_b"])
    row = out[out["id"] == 2].iloc[0]
    assert row["Speed_a"] == 30 and row["Accel_b"] == 4


def test_pivot_values_drops_undeclared_rows(mod):
    df = _base_df()
    df.loc[len(df)] = [3, "c", 99, 9]  # an undeclared sensor value
    out = _materialize(
        mod, df, index_columns=["id"], pivot_column="sensor", value_column="speed", pivot_values=["a", "b"],
    )
    assert sorted(out.columns) == ["a", "b", "id"]
    assert 3 not in set(out["id"])


def test_pivot_values_adds_stable_missing_column(mod):
    # Declare a pivot value ("c") that has zero matching rows this run --
    # the column must still appear (filled), not silently disappear.
    out = _materialize(
        mod,
        _base_df(),
        index_columns=["id"],
        pivot_column="sensor",
        value_column="speed",
        pivot_values=["a", "b", "c"],
        fill_value=0,
    )
    assert sorted(out.columns) == ["a", "b", "c", "id"]
    assert (out["c"] == 0).all()
