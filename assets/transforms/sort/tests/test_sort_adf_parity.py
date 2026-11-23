"""Tests for SortComponent's ADF-parity addition: an independent na_position per sort key
(ADF's Sort transform supports a null-handling flag per condition, not one for the whole
sort -- pandas' own `sort_values(na_position=...)` only accepts a single scalar).
"""
import math

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _df():
    return pd.DataFrame(
        {
            "grp": ["b", "a", None, "a", "b"],
            "val": [2, 1, 9, None, 1],
        }
    )


# ---------------------------------------------------------------------------
# _sort_with_per_column_na_position -- pure function
# ---------------------------------------------------------------------------


def test_per_column_na_position_grp_first_val_last(mod):
    out = mod._sort_with_per_column_na_position(_df(), ["grp", "val"], [True, True], ["first", "last"])
    # grp nulls FIRST, then grp asc (a, a, b, b); within each grp, val asc with nulls LAST
    assert out["grp"].tolist()[0] is None or (isinstance(out["grp"].tolist()[0], float) and math.isnan(out["grp"].tolist()[0]))
    rest = out.iloc[1:]
    assert rest["grp"].tolist() == ["a", "a", "b", "b"]
    # within grp "a": val 1, then NaN (nulls last for val)
    a_rows = rest[rest["grp"] == "a"]
    assert a_rows["val"].tolist()[0] == 1
    assert pd.isna(a_rows["val"].tolist()[1])
    # within grp "b": val 1, 2 ascending
    b_rows = rest[rest["grp"] == "b"]
    assert b_rows["val"].tolist() == [1, 2]


def test_per_column_na_position_both_last(mod):
    out = mod._sort_with_per_column_na_position(_df(), ["grp", "val"], [True, True], ["last", "last"])
    # grp nulls LAST now -- the None-grp row should be the final row
    assert pd.isna(out["grp"].iloc[-1])


# ---------------------------------------------------------------------------
# Component: build_defs + materialize
# ---------------------------------------------------------------------------


def _upstream(df):
    @dg.asset(name="upstream")
    def _upstream_asset():
        return df

    return _upstream_asset


def test_single_na_position_unchanged(mod):
    comp = mod.SortComponent(asset_name="out", upstream_asset_key="upstream", by=["val"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(_df()), *defs.assets])
    assert result.success


def test_na_position_per_column_via_component(mod):
    comp = mod.SortComponent(
        asset_name="out", upstream_asset_key="upstream", by=["grp", "val"],
        ascending=True, na_position_per_column=["first", "last"],
    )
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(_df()), *defs.assets])
    assert result.success
    out = result.asset_value("out")
    assert pd.isna(out["grp"].iloc[0])


def test_na_position_per_column_length_mismatch_raises(mod):
    comp = mod.SortComponent(
        asset_name="out", upstream_asset_key="upstream", by=["grp", "val"],
        na_position_per_column=["first"],
    )
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="must match"):
        dg.materialize([_upstream(_df()), *defs.assets])
