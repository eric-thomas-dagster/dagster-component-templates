"""Tests for ShopifyIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as zendesk_ingestion/servicenow_ingestion, for a dlt VERIFIED source
(shopify_source) with attribute-based resource access.

`asset_shape='multi_asset'` subsets dlt's own source object with
`source.with_resources(*names)` (a real dlt mechanism, live-verified before writing
this). `table_name_map` renames a resource's destination table via dlt's own
`DltResource.apply_hints(table_name=...)` (also live-verified).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        shop_url="https://my-shop.myshopify.com",
        private_app_password="tok",
        resources=["customers", "orders"],
    )
    kwargs.update(overrides)
    return mod.ShopifyIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "orders": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_customers"), dg.AssetKey("test_asset_orders")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    customers_df = result.output_for_node("test_asset", output_name="test_asset_customers")
    orders_df = result.output_for_node("test_asset", output_name="test_asset_orders")
    assert list(customers_df["id"]) == [1, 2]
    assert list(orders_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "orders": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customers")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("customers" in q for q in queries)
    assert not any("orders" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, resources=["customers"], table_name_map={"customers": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, resources=["customers"], asset_shape="multi_asset", table_name_map={"customers": "raw_customers"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_customers")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, resources=["customers"], table_name_map={"orders": "raw_orders"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
