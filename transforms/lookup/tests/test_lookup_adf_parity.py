"""Tests for LookupComponent's ADF-parity additions: `sort_conditions` (order candidate
matches before taking first/last, instead of relying on row order) and `match_flag_column`
(ADF's isMatch() -- a boolean column marking whether each row found a match).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _upstream(df, name="upstream"):
    @dg.asset(name=name)
    def _asset():
        return df

    return _asset


def _materialize(mod, df, lookup_df, **kwargs):
    kwargs.setdefault("asset_name", "out_asset")
    kwargs["upstream_asset_key"] = "upstream"
    kwargs["upstream_lookup_key"] = "lookup"
    comp = mod.LookupComponent(**kwargs)
    defs = comp.build_defs(context=None)
    result = dg.materialize([_upstream(df, "upstream"), _upstream(lookup_df, "lookup"), *defs.assets])
    assert result.success
    return result.asset_value("out_asset")


def test_sort_conditions_picks_highest_priority_match_first(mod):
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({"id": [1, 1, 1], "priority": [2, 3, 1], "label": ["b", "c", "a"]})
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["priority", "label"],
        multiple_match="first", sort_conditions=[{"column": "priority", "order": "asc"}],
    )
    assert out.iloc[0]["label"] == "a"
    assert out.iloc[0]["priority"] == 1


def test_sort_conditions_descending_with_last(mod):
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({"id": [1, 1, 1], "priority": [2, 3, 1], "label": ["b", "c", "a"]})
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["priority", "label"],
        multiple_match="last", sort_conditions=[{"column": "priority", "order": "desc"}],
    )
    # descending priority: c(3), b(2), a(1) -- "last" of that order is a
    assert out.iloc[0]["label"] == "a"


def test_match_flag_column_marks_matched_and_unmatched(mod):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    out = _materialize(mod, df, lookup_df, on=["id"], match_flag_column="was_matched")
    row1 = out[out["id"] == 1].iloc[0]
    row2 = out[out["id"] == 2].iloc[0]
    assert bool(row1["was_matched"]) is True
    assert bool(row2["was_matched"]) is False


def test_without_match_flag_column_no_extra_column(mod):
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    out = _materialize(mod, df, lookup_df, on=["id"])
    assert "_lookup_match" not in out.columns
