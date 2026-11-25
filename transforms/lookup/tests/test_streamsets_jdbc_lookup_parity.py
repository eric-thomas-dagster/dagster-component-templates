"""Tests for LookupComponent's StreamSets JDBC/Local Lookup-parity additions:
`multiple_match='all_as_list'` (ALL_AS_LIST -- one output row per left-side row, matches
collected into list columns, distinct from 'all' which is SPLIT_INTO_MULTIPLE_RECORDS) and
`on_no_match` ('pass' -- the default, matching Local Lookup's leave-the-field-unset behavior
as closely as a shared tabular column can; 'error' -- JDBC Lookup's fail-on-missing-match).
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


def test_all_as_list_collects_every_match_into_one_row(mod):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1, 1, 1, 2], "label": ["a", "b", "c", "x"]})
    out = _materialize(mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all_as_list")
    assert len(out) == 2  # one row per left-side row, not one per match
    row1 = out[out["id"] == 1].iloc[0]
    assert sorted(row1["label"]) == ["a", "b", "c"]
    row2 = out[out["id"] == 2].iloc[0]
    assert row2["label"] == ["x"]


def test_all_as_list_differs_from_all_cartesian_split(mod):
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({"id": [1, 1], "label": ["a", "b"]})
    split = _materialize(mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all")
    as_list = _materialize(mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all_as_list")
    assert len(split) == 2  # SPLIT_INTO_MULTIPLE_RECORDS: one row per match
    assert len(as_list) == 1  # ALL_AS_LIST: one row, list-valued column
    assert sorted(as_list.iloc[0]["label"]) == ["a", "b"]


def test_on_no_match_default_pass_leaves_nan(mod):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    out = _materialize(mod, df, lookup_df, on=["id"], columns_to_add=["label"])
    assert out[out["id"] == 1].iloc[0]["label"] == "found"
    assert pd.isna(out[out["id"] == 2].iloc[0]["label"])


def test_on_no_match_error_raises_when_any_row_unmatched(mod):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    comp = mod.LookupComponent(
        asset_name="out_asset", upstream_asset_key="upstream", upstream_lookup_key="lookup",
        on=["id"], columns_to_add=["label"], on_no_match="error",
    )
    defs = comp.build_defs(context=None)
    result = dg.materialize(
        [_upstream(df, "upstream"), _upstream(lookup_df, "lookup"), *defs.assets], raise_on_error=False
    )
    assert not result.success


def test_on_no_match_error_does_not_raise_when_all_rows_matched(mod):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1, 2], "label": ["a", "b"]})
    out = _materialize(mod, df, lookup_df, on=["id"], columns_to_add=["label"], on_no_match="error")
    assert list(out["label"]) == ["a", "b"]
