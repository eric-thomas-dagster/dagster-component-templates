"""`into` (the destination column name(s) in every `*_ops` entry) must accept
either a comma-separated string or a real list -- the shapes people (and
LLMs) naturally write it in.

Regression: a model wrote `split_ops`' `into` as `["first_name", "last_name"]`
instead of the documented `"first_name,last_name"` string. The old code did
`str(into).split(",")`, so the list's own repr became the input: splitting
`"['first_name', 'last_name']"` on commas produced two columns literally
named `"['first_name'"` and `" 'last_name']"` -- no error, just garbage
column names. Every other `*_ops` entry that takes a single-destination
`into` (numeric_ops, date_extract_ops, substring_ops, concat_ops,
case_when_ops, bin_ops, cumsum_ops, window_ops, count_match_ops) used `into`
directly as a column label, so a list there would raise instead of silently
corrupting -- but the fix normalizes all of them the same way, so any of
those shapes work too.

schema.json's documented split_ops example stays the string form
("first_name,last_name"); the fix just makes the list form work too.
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


# --- unit tests on the normalizer itself -----------------------------------

@pytest.mark.parametrize("raw,want", [
    (None, []),
    ("first_name,last_name", ["first_name", "last_name"]),
    (["first_name", "last_name"], ["first_name", "last_name"]),
    (["only"], ["only"]),
    ("only", ["only"]),
    (" first_name , , last_name ", ["first_name", "last_name"]),
    ([" first_name ", "", "last_name"], ["first_name", "last_name"]),
    ("", []),
    ([], []),
])
def test_normalize_into_multi(mod, raw, want):
    assert mod._normalize_into(raw, multi=True) == want


@pytest.mark.parametrize("raw,want", [
    (None, None),
    ("price_rounded", "price_rounded"),
    (["price_rounded"], "price_rounded"),
    (["price_rounded", "extra"], "price_rounded"),  # first wins, same as a single-dest field
    ("  price_rounded  ", "price_rounded"),
    ("", None),
    ([], None),
])
def test_normalize_into_single(mod, raw, want):
    assert mod._normalize_into(raw) == want


# --- split_ops (multi-destination) end to end --------------------------------

def test_split_ops_into_as_list_does_not_produce_garbage_columns(mod):
    df = pd.DataFrame({"full_name": ["Jo Doe", "An Lee"]})
    out = _materialize(
        mod, df,
        split_ops=json.dumps([{"column": "full_name", "delimiter": " ", "into": ["first_name", "last_name"]}]),
    )
    assert out["first_name"].tolist() == ["Jo", "An"]
    assert out["last_name"].tolist() == ["Doe", "Lee"]
    # the exact regression: the list's own repr leaking into column names
    assert not any(c.startswith("[") or c.endswith("]") for c in out.columns)


def test_split_ops_into_as_string_still_works(mod):
    df = pd.DataFrame({"full_name": ["Jo Doe", "An Lee"]})
    out = _materialize(
        mod, df,
        split_ops=json.dumps([{"column": "full_name", "delimiter": " ", "into": "first_name,last_name"}]),
    )
    assert out["first_name"].tolist() == ["Jo", "An"]
    assert out["last_name"].tolist() == ["Doe", "Lee"]


def test_split_ops_into_single_item_list(mod):
    df = pd.DataFrame({"sku": ["ABC", "XYZ"]})  # no '-' present -> one part
    out = _materialize(
        mod, df,
        split_ops=json.dumps([{"column": "sku", "delimiter": "-", "into": ["slug"]}]),
    )
    assert out["slug"].tolist() == ["ABC", "XYZ"]


def test_split_ops_into_extra_spaces_and_empty_entries(mod):
    df = pd.DataFrame({"full_name": ["Jo Doe"]})
    out = _materialize(
        mod, df,
        split_ops=json.dumps([{"column": "full_name", "delimiter": " ", "into": [" first_name ", "", "last_name"]}]),
    )
    assert out["first_name"].tolist() == ["Jo"]
    assert out["last_name"].tolist() == ["Doe"]


# --- a representative single-destination op (numeric_ops) -------------------

def test_numeric_ops_into_as_single_item_list(mod):
    df = pd.DataFrame({"amount": [150.456]})
    out = _materialize(
        mod, df,
        numeric_ops=json.dumps([{"column": "amount", "op": "round", "digits": 1, "into": ["amount_r"]}]),
    )
    assert out["amount_r"].tolist() == [150.5]
