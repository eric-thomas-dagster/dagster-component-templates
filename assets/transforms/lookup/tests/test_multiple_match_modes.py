"""Tests for the features ported from the old flat-layout `LookupComponent`
(transforms/lookup, since removed) when the two duplicate ADF-Lookup
components were consolidated into one: `multiple_match` ('first'/'last'/
'all'/'all_as_list'), `on_no_match`, `columns_to_add`, `rename`, and
`match_flag_column`.

Real `dg.materialize()` calls against real pandas/polars DataFrames -- nothing mocked.
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
        "region": ["us", "eu", "us", "us"],
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
    v = x.value if hasattr(x, "value") and hasattr(x, "metadata") else x
    return v.to_pandas() if hasattr(v, "to_pandas") else v


# ---------------------------------------------------------------------------
# multiple_match
# ---------------------------------------------------------------------------


def test_multiple_match_last_picks_opposite_of_first(mod):
    out_first = _as_df(_materialize(mod, match_sort_by=["effective_date"], match_sort_ascending=True, multiple_match="first")).set_index("order_id")
    out_last = _as_df(_materialize(mod, match_sort_by=["effective_date"], match_sort_ascending=True, multiple_match="last")).set_index("order_id")
    assert out_first.loc[1, "price"] == 10.0  # earliest, ascending, first
    assert out_last.loc[1, "price"] == 30.0  # latest, ascending, last


def test_multiple_match_all_fans_out_one_row_per_match(mod):
    out = _as_df(_materialize(mod, multiple_match="all"))
    # product A has 3 candidate rows, B has 1, Z has 0 (passes through once, null).
    # order_id 1 (A) and 3 (A) each fan out to 3 rows; order_id 2 (B) stays 1; order_id 4 (Z) stays 1 (null).
    assert len(out) == 3 + 3 + 1 + 1
    a_rows = out[out["order_id"] == 1]
    assert sorted(a_rows["price"].tolist()) == [10.0, 20.0, 30.0]
    z_row = out[out["order_id"] == 4]
    assert len(z_row) == 1
    assert pd.isna(z_row.iloc[0]["price"])


def test_multiple_match_all_as_list_collects_added_columns(mod):
    out = _as_df(_materialize(mod, multiple_match="all_as_list")).set_index("order_id")
    # Exactly one output row per primary row (no fan-out).
    assert len(out) == 4
    assert sorted(out.loc[1, "price"]) == [10.0, 20.0, 30.0]
    assert out.loc[2, "price"] == [99.0]
    # No match -> list column still present but empty (groupby never produced a Z key).
    z_price = out.loc[4, "price"]
    assert z_price is None or (isinstance(z_price, float) and pd.isna(z_price))


def test_multiple_match_invalid_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], multiple_match="bogus",
    )
    with pytest.raises(ValueError, match="multiple_match must be"):
        comp.build_defs(None)


# ---------------------------------------------------------------------------
# on_no_match
# ---------------------------------------------------------------------------


def test_on_no_match_pass_is_default_null_passthrough(mod):
    out = _as_df(_materialize(mod)).set_index("order_id")
    assert pd.isna(out.loc[4, "price"])


def test_on_no_match_error_raises_on_unmatched_row(mod):
    with pytest.raises(Exception, match="no match"):
        _materialize(mod, on_no_match="error")


def test_on_no_match_error_passes_when_all_matched(mod):
    # Drop order_id 4 (product Z, the only unmatched row) -- on_no_match='error' should not raise.
    orders_df = _orders()
    orders_df = orders_df[orders_df["order_id"] != 4]
    price_df = _price_history()
    comp = load_component_module().LookupComponent(
        asset_name="out_asset", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], on_no_match="error",
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [_upstream("orders", orders_df), _upstream("price_history", price_df)] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success


def test_on_no_match_invalid_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], on_no_match="bogus",
    )
    with pytest.raises(ValueError, match="on_no_match must be"):
        comp.build_defs(None)


# ---------------------------------------------------------------------------
# columns_to_add
# ---------------------------------------------------------------------------


def test_columns_to_add_restricts_attached_columns(mod):
    out = _as_df(_materialize(mod, columns_to_add=["price"]))
    assert "price" in out.columns
    assert "region" not in out.columns
    assert "effective_date" not in out.columns


def test_columns_to_add_in_is_match_mode_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], output_mode="is_match", columns_to_add=["price"],
    )
    with pytest.raises(ValueError, match="columns_to_add"):
        comp.build_defs(None)


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


def test_rename_renames_added_columns(mod):
    out = _as_df(_materialize(mod, rename={"price": "unit_price"}))
    assert "unit_price" in out.columns
    assert "price" not in out.columns


def test_rename_join_key_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], rename={"product_id": "sku"},
    )
    with pytest.raises(ValueError, match="join key"):
        comp.build_defs(None)


def test_rename_in_is_match_mode_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], output_mode="is_match", rename={"price": "unit_price"},
    )
    with pytest.raises(ValueError, match="rename"):
        comp.build_defs(None)


# ---------------------------------------------------------------------------
# match_flag_column (columns mode + a boolean flag simultaneously)
# ---------------------------------------------------------------------------


def test_match_flag_column_alongside_attached_columns(mod):
    out = _as_df(_materialize(mod, match_flag_column="found")).set_index("order_id")
    assert "price" in out.columns
    assert "found" in out.columns
    assert bool(out.loc[1, "found"]) is True
    assert bool(out.loc[4, "found"]) is False  # product Z, no match


def test_match_flag_column_in_is_match_mode_raises(mod):
    comp = mod.LookupComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], output_mode="is_match", match_flag_column="found",
    )
    with pytest.raises(ValueError, match="output_column"):
        comp.build_defs(None)


# ---------------------------------------------------------------------------
# polars backend parity for the ported features
# ---------------------------------------------------------------------------


def test_polars_multiple_match_all_fans_out(mod):
    out = _as_df(_materialize(mod, backend="polars", multiple_match="all"))
    assert len(out) == 3 + 3 + 1 + 1


def test_polars_multiple_match_all_as_list(mod):
    out = _as_df(_materialize(mod, backend="polars", multiple_match="all_as_list")).set_index("order_id")
    assert len(out) == 4
    assert sorted(out.loc[1, "price"]) == [10.0, 20.0, 30.0]


def test_polars_on_no_match_error_raises(mod):
    with pytest.raises(Exception, match="no match"):
        _materialize(mod, backend="polars", on_no_match="error")


def test_polars_columns_to_add_restricts(mod):
    out = _as_df(_materialize(mod, backend="polars", columns_to_add=["price"]))
    assert "price" in out.columns
    assert "region" not in out.columns


def test_polars_rename(mod):
    out = _as_df(_materialize(mod, backend="polars", rename={"price": "unit_price"}))
    assert "unit_price" in out.columns
    assert "price" not in out.columns


def test_polars_match_flag_column(mod):
    out = _as_df(_materialize(mod, backend="polars", match_flag_column="found")).set_index("order_id")
    assert "price" in out.columns
    assert bool(out.loc[1, "found"]) is True
    assert bool(out.loc[4, "found"]) is False
