"""asset_shape='multi_asset' correctness (one independently-materializable
asset per resource, with proof that an unselected resource is never built
or fetched) and table_name_map, across all three source_type values."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _sql_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="sql_database",
        connection_string="sqlite:///:memory:",
        resources=[{"name": "customers", "primary_key": "id"}, {"name": "orders", "primary_key": "order_id"}],
    )
    kwargs.update(overrides)
    return mod.DltLoadCollectionComponent(**kwargs)


def _rest_api_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="rest_api",
        client={"base_url": "https://api.example.com"},
        resources=[
            {"name": "customers", "endpoint": {"path": "customers"}},
            {"name": "orders", "endpoint": {"path": "orders"}},
        ],
    )
    kwargs.update(overrides)
    return mod.DltLoadCollectionComponent(**kwargs)


def _filesystem_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="filesystem",
        bucket_url="s3://fake-bucket",
        resources=[
            {"name": "customers", "file_glob": "customers/*.csv"},
            {"name": "orders", "file_glob": "orders/*.csv"},
        ],
    )
    kwargs.update(overrides)
    return mod.DltLoadCollectionComponent(**kwargs)


BUILDERS = [_sql_component, _rest_api_component, _filesystem_component]


@pytest.mark.parametrize("builder", BUILDERS)
def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch, builder):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "orders": pd.DataFrame({"order_id": [9]}),
    })
    component = builder(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_customers"), dg.AssetKey("test_asset_orders")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    customers_df = result.output_for_node("test_asset", output_name="test_asset_customers")
    orders_df = result.output_for_node("test_asset", output_name="test_asset_orders")
    assert list(customers_df["id"]) == [1, 2]
    assert list(orders_df["order_id"]) == [9]


@pytest.mark.parametrize("builder", BUILDERS)
def test_multi_asset_shape_selecting_one_resource_never_builds_the_other(mod, monkeypatch, builder):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1]}),
        "orders": pd.DataFrame({"order_id": [1]}),
    })
    component = builder(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customers")],
    )
    assert result.success, result
    # exactly one resource was ever constructed and handed to pipeline.run() --
    # 'orders' was never built, so it couldn't possibly have been fetched.
    run_call = fake_pipeline.run_calls[-1]
    names = sorted(run_call.resources.keys()) if hasattr(run_call, "resources") else sorted(r.name for r in run_call)
    assert names == ["customers"]
    queries = fake_pipeline.sql_client_instance.queries
    assert any("customers" in q for q in queries)
    assert not any("orders" in q for q in queries)


@pytest.mark.parametrize("builder", BUILDERS)
def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch, builder):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]}), "orders": pd.DataFrame()})
    component = builder(mod, table_name_map={"customers": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    df = result.output_for_node("test_asset")
    assert len(df) == 3


@pytest.mark.parametrize("builder", BUILDERS)
def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch, builder):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = builder(mod, asset_shape="multi_asset", table_name_map={"customers": "raw_customers"})
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
    component = _rest_api_component(mod, table_name_map={"unknown_resource": "raw_x"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
