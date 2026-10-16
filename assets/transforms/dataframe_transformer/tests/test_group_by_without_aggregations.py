"""group_by with no agg_functions must mean "distinct combinations of the group
columns" (SQL GROUP BY semantics) instead of being silently ignored -- the
Transform UI lets you pick only a group-by column, and its preview shows the
distinct groups, so the materialized asset has to match."""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, df, **attrs):
    @dg.asset(name="raw")
    def raw():
        return df

    component = mod.DataFrameTransformerComponent(
        asset_name="out", upstream_asset_key="raw", **attrs,
    )
    asset_def = list(component.build_defs(None).assets)[0]
    result = dg.materialize([asset_def, raw])
    assert result.success
    return result.output_for_node("out")


PEOPLE = pd.DataFrame({
    "first_name": ["Ann", "Bob", "Cy", "Di", "Ed"],
    "last_name": ["Smith", "Jones", "Smith", "Lee", "Jones"],
    "amount": [1, 2, 3, 4, 5],
})


def test_group_by_alone_returns_distinct_groups(mod):
    out = _materialize(mod, PEOPLE, group_by="last_name")
    assert list(out.columns) == ["last_name"]
    assert sorted(out["last_name"]) == ["Jones", "Lee", "Smith"]


def test_group_by_multiple_columns_without_aggregations(mod):
    df = pd.DataFrame({"a": [1, 1, 2, 2, 2], "b": ["x", "x", "y", "y", "z"], "n": range(5)})
    out = _materialize(mod, df, group_by="a,b")
    assert out[["a", "b"]].to_dict("records") == [
        {"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 2, "b": "z"},
    ]


def test_group_by_with_aggregations_is_unchanged(mod):
    out = _materialize(mod, PEOPLE, group_by="last_name", agg_functions=json.dumps({"amount": "sum"}))
    assert dict(zip(out["last_name"], out["amount"])) == {"Jones": 7, "Lee": 4, "Smith": 4}


def test_group_by_unknown_column_is_ignored(mod):
    out = _materialize(mod, PEOPLE, group_by="nope")
    assert len(out) == len(PEOPLE)


# --- aggregations WITHOUT group_by: one row over the whole table ---------------

def test_aggregation_without_group_by_aggregates_the_whole_table(mod):
    out = _materialize(mod, PEOPLE, agg_functions=json.dumps({"amount": "max"}))
    assert len(out) == 1
    assert out.iloc[0]["amount"] == 5


def test_multiple_whole_table_aggregations(mod):
    out = _materialize(mod, PEOPLE, agg_functions=json.dumps({"amount": "sum", "last_name": "count"}))
    assert out.to_dict("records") == [{"amount": 15, "last_name": 5}]


def test_whole_table_aggregation_ignores_unknown_columns(mod):
    out = _materialize(mod, PEOPLE, agg_functions=json.dumps({"nope": "max"}))
    assert len(out) == len(PEOPLE)
