"""asset_shape ('single' default / 'multi_asset') and table_name_map, added to
DltRestApiSourceComponent in the same rollout as zendesk_ingestion/
servicenow_ingestion/github_ingestion/dlt_load_collection."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        client={"base_url": "https://api.example.com"},
        resources=[
            {"name": "customers", "endpoint": {"path": "customers"}},
            {"name": "orders", "endpoint": {"path": "orders"}},
        ],
    )
    kwargs.update(overrides)
    return mod.DltRestApiSourceComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    fake_pipeline, _ = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "orders": pd.DataFrame({"order_id": [9]}),
    })
    component = _component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_customers"), dg.AssetKey("test_asset_orders")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    customers_df = result.output_for_node("test_asset", output_name="test_asset_customers")
    orders_df = result.output_for_node("test_asset", output_name="test_asset_orders")
    assert list(customers_df["id"]) == [1, 2]
    assert list(orders_df["order_id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_requests_the_other(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1]}),
        "orders": pd.DataFrame({"order_id": [1]}),
    })
    component = _component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customers")],
    )
    assert result.success, result
    # the config handed to rest_api_source() only ever contained the selected resource --
    # 'orders' was never even requested, not just filtered out after the fact.
    names = [r["name"] for r in captured["config"]["resources"]]
    assert names == ["customers"]
    queries = fake_pipeline.sql_client_instance.queries
    assert any("customers" in q for q in queries)
    assert not any("orders" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]}), "orders": pd.DataFrame()})
    component = _component(mod, table_name_map={"customers": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    config_names = {r["name"]: r.get("table_name") for r in captured["config"]["resources"]}
    assert config_names["customers"] == "raw_customers"
    assert config_names["orders"] is None
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = _component(mod, asset_shape="multi_asset", table_name_map={"customers": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customers")],
    )
    assert result.success, result
    df = result.output_for_node("test_asset", output_name="test_asset_customers")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _component(mod, table_name_map={"unknown_resource": "raw_x"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
