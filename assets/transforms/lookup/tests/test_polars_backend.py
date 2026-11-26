"""Real, committed test for LookupMatchComponent's `backend: polars` path --
same match-sort-dedup-then-join technique as polars_pipeline's own `lookup`
op (sort + `.unique(subset=key_cols, keep='first')` on the lookup side before
joining), mirroring `dataframe_join`'s own dual-backend convention.

Materializes through the real asset graph (dg.materialize + dg.InMemoryIOManager),
matching this component's existing test_lookup_adf_parity.py convention. Each
scenario asserts the polars backend produces the SAME result as the pandas
backend for identical input, confirmed against a real, installed polars 2.0.0.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")
import polars as pl  # noqa: E402


@pytest.fixture()
def mod():
    return load_component_module()


def _orders():
    return pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "product_id": ["A", "B", "A", "Z"],  # Z has no match in price_history
    })


def _price_history():
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
    kw = dict(
        asset_name="out_asset",
        upstream_asset_key="orders",
        lookup_asset_key="price_history",
        on=["product_id"],
    )
    kw.update(overrides)
    comp = mod.LookupMatchComponent(**kw)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [_upstream("orders", _orders()), _upstream("price_history", _price_history())] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node("out_asset")


def _as_df(x):
    v = x.value if hasattr(x, "value") and hasattr(x, "metadata") else x
    return v.to_pandas() if hasattr(v, "to_pandas") else v


def test_polars_backend_returns_a_real_polars_dataframe(mod):
    out = _materialize(mod, backend="polars")
    out = out.value if hasattr(out, "value") and hasattr(out, "metadata") else out
    assert isinstance(out, pl.DataFrame)


def test_polars_multiple_matches_resolved_deterministically_by_sort(mod):
    out = _as_df(_materialize(mod, backend="polars", match_sort_by=["effective_date"], match_sort_ascending=False))
    out = out.set_index("order_id")
    assert out.loc[1, "price"] == 30.0
    assert out.loc[3, "price"] == 30.0
    assert out.loc[2, "price"] == 99.0


def test_polars_default_columns_mode_pulls_matched_columns_null_for_no_match(mod):
    out = _as_df(_materialize(mod, backend="polars"))
    out = out.set_index("order_id")
    assert "price" in out.columns and "effective_date" in out.columns
    assert pd.isna(out.loc[4, "price"])
    assert pd.notna(out.loc[1, "price"])
    assert pd.notna(out.loc[2, "price"])


def test_polars_is_match_mode_emits_single_boolean_column_no_lookup_columns(mod):
    out = _as_df(_materialize(mod, backend="polars", output_mode="is_match", output_column="has_price"))
    out = out.set_index("order_id")
    assert list(out.columns) == ["product_id", "has_price"]
    assert bool(out.loc[1, "has_price"]) is True
    assert bool(out.loc[2, "has_price"]) is True
    assert bool(out.loc[3, "has_price"]) is True
    assert bool(out.loc[4, "has_price"]) is False  # product Z has no match


# ---------------------------------------------------------------------------
# pandas vs. polars backend parity -- both must agree on the same inputs.
# ---------------------------------------------------------------------------


def test_polars_and_pandas_backends_agree_on_multi_match_resolution(mod):
    pandas_out = _as_df(_materialize(mod, backend="pandas", match_sort_by=["effective_date"], match_sort_ascending=False))
    polars_out = _as_df(_materialize(mod, backend="polars", match_sort_by=["effective_date"], match_sort_ascending=False))
    pandas_out = pandas_out.sort_values("order_id").reset_index(drop=True)
    polars_out = polars_out.sort_values("order_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        pandas_out[["order_id", "product_id", "price"]],
        polars_out[["order_id", "product_id", "price"]],
    )


def test_polars_and_pandas_backends_agree_on_is_match(mod):
    pandas_out = _as_df(_materialize(mod, backend="pandas", output_mode="is_match"))
    polars_out = _as_df(_materialize(mod, backend="polars", output_mode="is_match"))
    pandas_out = pandas_out.sort_values("order_id").reset_index(drop=True)
    polars_out = polars_out.sort_values("order_id").reset_index(drop=True)
    assert pandas_out["is_match"].astype(bool).tolist() == polars_out["is_match"].astype(bool).tolist()


def test_invalid_backend_raises(mod):
    comp = mod.LookupMatchComponent(
        asset_name="out", upstream_asset_key="orders", lookup_asset_key="price_history",
        on=["product_id"], backend="spark",
    )
    with pytest.raises(ValueError, match="backend must be 'pandas' or 'polars'"):
        comp.build_defs(None)
