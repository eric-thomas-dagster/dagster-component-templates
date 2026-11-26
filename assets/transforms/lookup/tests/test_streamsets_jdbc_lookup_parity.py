"""Tests for LookupComponent's StreamSets JDBC/Local Lookup-parity additions:
`multiple_match='all_as_list'` (ALL_AS_LIST -- one output row per left-side row, matches
collected into list columns, distinct from 'all' which is SPLIT_INTO_MULTIPLE_RECORDS) and
`on_no_match` ('pass' -- the default, matching Local Lookup's leave-the-field-unset behavior
as closely as a shared tabular column can; 'error' -- JDBC Lookup's fail-on-missing-match).

Also covers backward compatibility with the pre-consolidation field names
(`upstream_lookup_key` instead of `lookup_asset_key`) -- every test below runs
once against each spelling.
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


def _materialize(mod, df, lookup_df, *, legacy_field_names=False, **kwargs):
    kwargs.setdefault("asset_name", "out_asset")
    kwargs["upstream_asset_key"] = "upstream"
    if legacy_field_names:
        kwargs["upstream_lookup_key"] = "lookup"
    else:
        kwargs["lookup_asset_key"] = "lookup"
    comp = mod.LookupComponent(**kwargs)
    defs = comp.build_defs(None)
    result = dg.materialize([_upstream(df, "upstream"), _upstream(lookup_df, "lookup"), *defs.assets])
    assert result.success
    return result.asset_value("out_asset")


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_all_as_list_collects_every_match_into_one_row(mod, legacy_field_names):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1, 1, 1, 2], "label": ["a", "b", "c", "x"]})
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all_as_list",
        legacy_field_names=legacy_field_names,
    )
    assert len(out) == 2  # one row per left-side row, not one per match
    row1 = out[out["id"] == 1].iloc[0]
    assert sorted(row1["label"]) == ["a", "b", "c"]
    row2 = out[out["id"] == 2].iloc[0]
    assert row2["label"] == ["x"]


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_all_as_list_differs_from_all_cartesian_split(mod, legacy_field_names):
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({"id": [1, 1], "label": ["a", "b"]})
    split = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all",
        legacy_field_names=legacy_field_names,
    )
    as_list = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["label"], multiple_match="all_as_list",
        legacy_field_names=legacy_field_names,
    )
    assert len(split) == 2  # SPLIT_INTO_MULTIPLE_RECORDS: one row per match
    assert len(as_list) == 1  # ALL_AS_LIST: one row, list-valued column
    assert sorted(as_list.iloc[0]["label"]) == ["a", "b"]


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_on_no_match_default_pass_leaves_nan(mod, legacy_field_names):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["label"], legacy_field_names=legacy_field_names,
    )
    assert out[out["id"] == 1].iloc[0]["label"] == "found"
    assert pd.isna(out[out["id"] == 2].iloc[0]["label"])


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_on_no_match_error_raises_when_any_row_unmatched(mod, legacy_field_names):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1], "label": ["found"]})
    kwargs = dict(
        asset_name="out_asset", upstream_asset_key="upstream",
        on=["id"], columns_to_add=["label"], on_no_match="error",
    )
    kwargs["upstream_lookup_key" if legacy_field_names else "lookup_asset_key"] = "lookup"
    comp = mod.LookupComponent(**kwargs)
    defs = comp.build_defs(None)
    result = dg.materialize(
        [_upstream(df, "upstream"), _upstream(lookup_df, "lookup"), *defs.assets], raise_on_error=False
    )
    assert not result.success


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_on_no_match_error_does_not_raise_when_all_rows_matched(mod, legacy_field_names):
    df = pd.DataFrame({"id": [1, 2]})
    lookup_df = pd.DataFrame({"id": [1, 2], "label": ["a", "b"]})
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["label"], on_no_match="error",
        legacy_field_names=legacy_field_names,
    )
    assert list(out["label"]) == ["a", "b"]


def test_legacy_sort_conditions_maps_to_match_sort_by(mod):
    """sort_conditions=[{column, order}] (pre-consolidation shape) picks the
    same winning row as the equivalent match_sort_by/match_sort_ascending."""
    df = pd.DataFrame({"id": [1]})
    lookup_df = pd.DataFrame({
        "id": [1, 1, 1],
        "effective_date": ["2024-01-01", "2024-06-01", "2024-03-01"],
        "price": [10.0, 30.0, 20.0],
    })
    out = _materialize(
        mod, df, lookup_df, on=["id"], columns_to_add=["price"],
        sort_conditions=[{"column": "effective_date", "order": "desc"}],
        legacy_field_names=True,
    )
    assert out.iloc[0]["price"] == 30.0  # 2024-06-01 is the latest effective_date
