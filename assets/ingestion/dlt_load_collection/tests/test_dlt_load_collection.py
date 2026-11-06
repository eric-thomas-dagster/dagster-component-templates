"""Core single-shape behavior for DltLoadCollectionComponent, across all three
source_type values."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


def _names_from_run_call(run_call):
    """`run_call` is either a List[DltResource] (sql_database/filesystem --
    each resource is built independently) or a single DltSource (rest_api --
    its resources share one client/auth context)."""
    if hasattr(run_call, "resources"):
        return sorted(run_call.resources.keys())
    return sorted(r.name for r in run_call)


@pytest.fixture()
def mod():
    return load_component_module()


def _sql_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="sql_database",
        connection_string="sqlite:///:memory:",
        sql_resources=[{"name": "customers", "primary_key": "id"}, {"name": "orders", "primary_key": "order_id"}],
    )
    kwargs.update(overrides)
    return mod.DltLoadCollectionComponent(**kwargs)


def _rest_api_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="rest_api",
        client={"base_url": "https://api.example.com", "auth": {"type": "bearer", "token": "x"}},
        rest_api_resources=[
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
        filesystem_bucket_url="s3://fake-bucket",
        filesystem_resources=[
            {"name": "customers", "file_glob": "customers/*.csv"},
            {"name": "orders", "file_glob": "orders/*.parquet", "file_format": "parquet"},
        ],
    )
    kwargs.update(overrides)
    return mod.DltLoadCollectionComponent(**kwargs)


def test_single_shape_is_the_default(mod):
    component = _rest_api_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


@pytest.mark.parametrize("builder", [_sql_component, _rest_api_component, _filesystem_component])
def test_single_shape_unions_every_resource(mod, monkeypatch, builder):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "orders": pd.DataFrame({"order_id": [9]}),
    })
    component = builder(mod)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    df = result.output_for_node("test_asset")
    assert len(df) == 3
    assert set(df["_resource_type"]) == {"customers", "orders"}
    # exactly the two resources were handed to pipeline.run() -- nothing extra was built.
    assert _names_from_run_call(fake_pipeline.run_calls[-1]) == ["customers", "orders"]


def test_rest_api_resources_each_appear_as_a_real_named_dlt_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1]}),
        "orders": pd.DataFrame({"order_id": [1]}),
    })
    component = _rest_api_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert _names_from_run_call(fake_pipeline.run_calls[-1]) == ["customers", "orders"]


def test_filesystem_resources_each_appear_as_a_real_named_dlt_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1]}),
        "orders": pd.DataFrame({"order_id": [1]}),
    })
    component = _filesystem_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert _names_from_run_call(fake_pipeline.run_calls[-1]) == ["customers", "orders"]


def test_sql_database_resources_each_appear_as_a_real_named_dlt_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1]}),
        "orders": pd.DataFrame({"order_id": [1]}),
    })
    component = _sql_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert _names_from_run_call(fake_pipeline.run_calls[-1]) == ["customers", "orders"]


def test_persist_only_emits_materialize_result(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1]}), "orders": pd.DataFrame()})
    component = _rest_api_component(mod, destination="snowflake", persist_only=True, destination_credentials_env_var="X")
    import os
    os.environ.setdefault("X", "snowflake://fake")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_non_sql_destination_warns_and_emits_materialize_result(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _rest_api_component(mod, destination="filesystem", bucket_url="s3://dest-bucket")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_no_data_returns_empty_dataframe(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={})
    component = _rest_api_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 0
