"""Committed regression tests for two real bugs found and fixed while
bringing DataFrameTransformerComponent up to parity with
SqlTransformerComponent's op vocabulary (Dagster Designer's Transform UI
routes to whichever of the two backends matches the source):

1. A NameError bug: `_effective_lineage`'s column-lineage auto-infer block
   referenced a bare `upstream_asset_key` name that was never assigned as a
   local in `build_defs` -- only `self.upstream_asset_key` existed. That
   branch is truthy on almost every real materialize with at least one
   passthrough column, so this crashed nearly every real run.
2. 14 fields (replace_ops, split_ops, window_ops, count_match_ops,
   case_when_ops, concat_ops, date_extract_ops, substring_ops, numeric_ops,
   sample_config, bin_ops, dedupe_subset, cumsum_ops, fill_direction_ops)
   were being sent to this component by Dagster Designer's backend but were
   never implemented here -- silently ignored, no error, no effect. Every
   test below materializes the real asset via dg.materialize() (not a mock
   context) and asserts on the real output DataFrame.

Also covers the two smaller additions layered on top: string_operations'
new "remove_punctuation" mode and its "*" (all string columns) wildcard,
matching data_cleansing's own auto-detect behavior.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, df: pd.DataFrame, **attrs):
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


def test_lineage_auto_infer_does_not_crash_with_no_explicit_config(mod):
    # This is the NameError regression: any transform with at least one
    # passthrough column (the common case -- no explicit column_lineage set)
    # used to crash here, before ever reaching a real op.
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    out = _materialize(mod, df)
    assert len(out) == 2


def test_numeric_date_extract_substring_concat_ops(mod):
    df = pd.DataFrame({
        "amount": [150.456, 20.1],
        "order_date": ["2024-03-05", "2024-01-01"],
        "sku": ["ABC123", "XYZ999"],
        "first": ["Jo", "An"],
        "last": ["Doe", "Lee"],
    })
    out = _materialize(
        mod, df,
        numeric_ops=json.dumps([{"column": "amount", "op": "round", "digits": 1, "into": "amount_r"}]),
        date_extract_ops=json.dumps([{"column": "order_date", "part": "month", "into": "order_month"}]),
        substring_ops=json.dumps([{"column": "sku", "start": 1, "length": 3, "into": "sku_prefix"}]),
        concat_ops=json.dumps([{"columns": "first,last", "separator": " ", "into": "full_name"}]),
    )
    assert out["amount_r"].tolist() == [150.5, 20.1]
    assert out["order_month"].tolist() == [3, 1]
    assert out["sku_prefix"].tolist() == ["ABC", "XYZ"]
    assert out["full_name"].tolist() == ["Jo Doe", "An Lee"]


def test_string_operations_remove_punctuation_and_wildcard(mod):
    df = pd.DataFrame({"name": ["Mr. John!!", "  jane-doe  "], "code": ["A-1", "B-2"]})
    out = _materialize(
        mod, df,
        string_operations=json.dumps([
            {"column": "*", "operation": "trim"},
            {"column": "name", "operation": "remove_punctuation"},
        ]),
    )
    assert out["name"].tolist() == ["Mr John", "janedoe"]
    # wildcard trim also hit 'code' (untouched by remove_punctuation, which
    # only targeted 'name' explicitly).
    assert out["code"].tolist() == ["A-1", "B-2"]


def test_window_ops_rank_and_row_number(mod):
    df = pd.DataFrame({"cat": ["a", "a", "b"], "amount": [10, 30, 20]})
    out = _materialize(
        mod, df,
        window_ops=json.dumps([
            {"kind": "rank", "orderBy": "amount", "partitionBy": "cat", "orderAsc": False, "into": "rnk"},
        ]),
    )
    ranked = dict(zip(out["amount"], out["rnk"]))
    assert ranked[30] == 1  # highest amount in its partition ranks first
    assert ranked[10] == 2


def test_case_when_and_bin_ops(mod):
    df = pd.DataFrame({"amount": [5, 50, 500]})
    out = _materialize(
        mod, df,
        case_when_ops=json.dumps([{
            "branches": [{"column": "amount", "operator": "greater_than", "value": "100", "then": "large"}],
            "else": "small", "into": "bucket",
        }]),
        bin_ops=json.dumps([{"column": "amount", "boundaries": "10,100", "labels": "low,mid,high", "into": "amount_bin"}]),
    )
    assert out["bucket"].tolist() == ["small", "small", "large"]
    assert out["amount_bin"].astype(str).tolist() == ["low", "mid", "high"]


def test_cumsum_and_fill_direction_ops_respect_partition_and_order(mod):
    df = pd.DataFrame({
        "cat": ["a", "a", "b", "b"],
        "day": [2, 1, 1, 2],
        "amount": [20, 10, 5, None],
        "price": [None, 3.0, 7.0, None],
    })
    out = _materialize(
        mod, df,
        cumsum_ops=json.dumps([{"column": "amount", "partitionBy": "cat", "orderBy": "day", "into": "running"}]),
        fill_direction_ops=json.dumps([{"column": "price", "direction": "ffill", "partitionBy": "cat", "orderBy": "day"}]),
    )
    by_day = out.set_index(["cat", "day"])
    assert by_day.loc[("a", 1), "running"] == 10
    assert by_day.loc[("a", 2), "running"] == 30
    # price forward-filled within cat 'a': day1=3.0 carries to day2.
    assert by_day.loc[("a", 2), "price"] == 3.0


def test_dedupe_subset_and_replace_ops_and_split_ops(mod):
    df = pd.DataFrame({
        "cat": ["a", "a", "b"],
        "sku": ["X-1", "X-2", "Y-1"],
        "name": ["other", "Mr. Smith!!", "other"],
    })
    out = _materialize(
        mod, df,
        dedupe_subset=json.dumps({"subsetCols": "cat", "keep": "last"}),
        replace_ops=json.dumps([{"column": "name", "find": "!!", "replace": ""}, {"column": "name", "find": "Mr.", "replace": ""}]),
        split_ops=json.dumps([{"column": "sku", "delimiter": "-", "into": "prefix,suffix"}]),
    )
    assert len(out) == 2  # deduped on cat, kept LAST per group -- row 0 (cat=a) dropped
    # the surviving cat='a' row is the one with "Mr. Smith!!" (index 1, last
    # in its group), and replace_ops ran BEFORE dedupe in the op pipeline,
    # so its cleaned value should be present.
    assert " Smith" in out["name"].tolist()
    assert set(out["prefix"]) <= {"X", "Y"}


def test_count_match_ops_and_sample_config(mod):
    df = pd.DataFrame({"cat": ["a", "a", "b", "b", "b"]})
    out = _materialize(
        mod, df,
        count_match_ops=json.dumps([{"column": "cat", "operator": "equals", "value": "a", "into": "n_a", "partitionBy": "cat"}]),
        sample_config=json.dumps({"n": 2, "random": False}),
    )
    assert len(out) == 2
    assert set(out["n_a"].tolist()) <= {0, 2}
