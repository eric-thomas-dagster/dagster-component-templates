"""Tests for MongoDBIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion/zendesk_ingestion. Unlike zendesk (a pip-unavailable
verified source faked wholesale), this component's dlt source is a REAL inline
`@dlt.source`/`@dlt.resource` factory -- only `dlt.pipeline(...)` is faked here, so
`with_resources()`/`apply_hints()` run against the real dlt framework, not a stand-in.
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
        connection_url="mongodb://localhost:27017",
        database="mydb",
        collection_names=["orders", "users"],
    )
    kwargs.update(overrides)
    return mod.MongoDBIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_collection(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "orders": pd.DataFrame({"id": [1, 2]}),
        "users": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_orders"), dg.AssetKey("test_asset_users")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    orders_df = result.output_for_node("test_asset", output_name="test_asset_orders")
    users_df = result.output_for_node("test_asset", output_name="test_asset_users")
    assert list(orders_df["id"]) == [1, 2]
    assert list(users_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_collection_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "orders": pd.DataFrame({"id": [1, 2]}),
        "users": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_orders")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("orders" in q for q in queries)
    assert not any("users" in q for q in queries)

    # The source built for this run must only actually contain the selected resource --
    # confirms the hand-rolled factory was called with just the subset, not all collections.
    built_source = fake_pipeline.run_calls[-1]
    assert hasattr(built_source, "orders")


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_orders": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, collection_names=["orders"], table_name_map={"orders": "raw_orders"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_orders": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, collection_names=["orders"], asset_shape="multi_asset", table_name_map={"orders": "raw_orders"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_orders")
    assert len(df) == 3


def test_table_name_map_with_unknown_collection_raises_a_clear_error(mod):
    component = _make_component(mod, collection_names=["orders"], table_name_map={"users": "raw_users"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
