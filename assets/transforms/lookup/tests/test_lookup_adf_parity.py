"""Tests for LookupComponent -- ADF's Lookup data flow transform: for each row
in a primary stream, find a matching row in a secondary/lookup stream by key
column(s), with two features a plain left join (dataframe_join) can't express:

  1. Deterministic multiple-match resolution via `match_sort_by` /
     `match_sort_ascending` (sort candidates, take exactly one).
  2. `output_mode='is_match'` -- a single boolean column instead of the
     matched row's columns.

Real `dg.materialize()` calls against real pandas DataFrames -- nothing mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _orders():
    return pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "product_id": ["A", "B", "A", "Z"],  # Z has no match in price_history
    })


def _price_history():
    # Product A has THREE candidate price rows -- the multiple-match case.
    # Product B has exactly one.
    return pd.DataFrame({
        "product_id": ["A", "A", "A", "B"],
        "effective_date": ["2024-01-01", "2024-06-01", "2024-03-01", "2024-01-01"],
        "price": [10.0, 30.0, 20.0, 99.0],
    })


def _upstream(name, df):
    @dg.asset(name=name)
    def _a():
        return df
    return _a


def _materialize(mod, **overrides):
    orders_df = _orders()
    price_df = _price_history()
    kw = dict(
        asset_name="out_asset",
        upstream_asset_key="orders",
        lookup_asset_key="price_history",
        on=["product_id"],
    )
    kw.update(overrides)
    comp = mod.LookupComponent(**kw)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [_upstream("orders", orders_df), _upstream("price_history", price_df)] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node("out_asset")


def _as_df(x):
    return x.value if hasattr(x, "value") and hasattr(x, "metadata") else x


# ---------------------------------------------------------------------------
# Pure function: _resolve_one_match_per_key
# ---------------------------------------------------------------------------


def test_resolve_one_match_per_key_sorts_then_takes_first(mod):
    lookup = _price_history()
    # Most recent effective_date wins: sort descending, take first -> 2024-06-01 / 30.0
    resolved = mod._resolve_one_match_per_key(
        lookup, ["product_id"], ["effective_date"], False
    )
    row_a = resolved[resolved["product_id"] == "A"].iloc[0]
    assert row_a["effective_date"] == "2024-06-01"
    assert row_a["price"] == 30.0
    assert len(resolved) == 2  # one row per distinct product_id (A, B)


def test_resolve_one_match_per_key_ascending_picks_earliest(mod):
    lookup = _price_history()
    resolved = mod._resolve_one_match_per_key(
        lookup, ["product_id"], ["effective_date"], True
    )
    row_a = resolved[resolved["product_id"] == "A"].iloc[0]
    assert row_a["effective_date"] == "2024-01-01"
    assert row_a["price"] == 10.0


def test_resolve_one_match_per_key_no_sort_is_first_occurrence(mod):
    # Undocumented-order case: no sort_by -> first occurrence in the given
    # frame order (pandas drop_duplicates(keep='first')), not an arbitrary
    # crash and not a fan-out. For product A, the first row in _price_history()
    # (as constructed) is effective_date=2024-01-01 / price=10.0.
    lookup = _price_history()
    resolved = mod._resolve_one_match_per_key(lookup, ["product_id"], None, None)
    row_a = resolved[resolved["product_id"] == "A"].iloc[0]
    assert row_a["effective_date"] == "2024-01-01"
    assert row_a["price"] == 10.0


def test_resolve_one_match_per_key_ascending_list_length_mismatch_raises(mod):
    lookup = _price_history()
    with pytest.raises(ValueError, match="must match"):
        mod._resolve_one_match_per_key(lookup, ["product_id"], ["effective_date", "price"], [True])


# ---------------------------------------------------------------------------
# Component: build_defs + materialize
# ---------------------------------------------------------------------------


def test_multiple_matches_resolved_deterministically_by_sort(mod):
    out = _as_df(_materialize(mod, match_sort_by=["effective_date"], match_sort_ascending=False))
    out = out.set_index("order_id")
    # order_id 1 and 3 both have product_id=A -> most-recent price (30.0) for both.
    assert out.loc[1, "price"] == 30.0
    assert out.loc[3, "price"] == 30.0
    # order_id 2 has product_id=B -> its single candidate.
    assert out.loc[2, "price"] == 99.0


def test_match_sort_by_column_excluded_from_columns_to_add(mod):
    """Regression: `match_sort_by=['effective_date']` with `columns_to_add=['price']`
    (effective_date requested for sorting only, not as an output column) must not
    raise -- the sort-only column has to survive the lookup-side column subset
    long enough to sort by, then get dropped before the merge."""
    out = _as_df(_materialize(
        mod, columns_to_add=["price"], match_sort_by=["effective_date"], match_sort_ascending=False,
    ))
    out = out.set_index("order_id")
    assert "effective_date" not in out.columns
    assert out.loc[1, "price"] == 30.0
    assert out.loc[3, "price"] == 30.0
    assert out.loc[2, "price"] == 99.0


def test_default_columns_mode_pulls_matched_columns_null_for_no_match(mod):
    out = _as_df(_materialize(mod))
    out = out.set_index("order_id")
    assert "price" in out.columns and "effective_date" in out.columns
    # order_id 4 -> product_id Z has no match in price_history: null, not dropped/erroring.
    assert pd.isna(out.loc[4, "price"])
    assert pd.isna(out.loc[4, "effective_date"])
    # Matched rows actually got a real (non-null) price.
    assert pd.notna(out.loc[1, "price"])
    assert pd.notna(out.loc[2, "price"])


def test_is_match_mode_emits_single_boolean_column_no_lookup_columns(mod):
    out = _as_df(_materialize(mod, output_mode="is_match", output_column="has_price"))
    out = out.set_index("order_id")
    assert list(out.columns) == ["product_id", "has_price"]
    assert out.loc[1, "has_price"] == True  # noqa: E712
    assert out.loc[2, "has_price"] == True  # noqa: E712
    assert out.loc[3, "has_price"] == True  # noqa: E712
    assert out.loc[4, "has_price"] == False  # noqa: E712 -- product Z has no match


def test_is_match_mode_default_output_column_name(mod):
    out = _as_df(_materialize(mod, output_mode="is_match"))
    assert "is_match" in out.columns


def test_output_mode_invalid_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], output_mode="bogus",
    )
    with pytest.raises(ValueError, match="output_mode must be"):
        comp.build_defs(None)


def test_requires_join_keys(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
    )
    with pytest.raises(ValueError, match="set `on`"):
        comp.build_defs(None)


def test_left_on_right_on_different_names(mod):
    price_renamed = _price_history().rename(columns={"product_id": "sku"})

    @dg.asset(name="price_history_renamed")
    def _price_renamed():
        return price_renamed

    comp = mod.LookupComponent(
        asset_name="out_renamed", upstream_asset_key="orders", lookup_asset_key="price_history_renamed",
        left_on=["product_id"], right_on=["sku"], match_sort_by=["effective_date"], match_sort_ascending=False,
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [_upstream("orders", _orders()), _price_renamed] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    out = _as_df(result.output_for_node("out_renamed")).set_index("order_id")
    assert out.loc[1, "price"] == 30.0
    assert "sku" not in out.columns  # lookup-side duplicate key dropped
