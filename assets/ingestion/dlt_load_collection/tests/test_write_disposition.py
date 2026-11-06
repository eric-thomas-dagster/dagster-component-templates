"""Per-resource write_disposition. Real gap reported from production usage:
without it, every materialization APPENDS, so re-materializing the same
partition duplicates rows (verified: 2 materializations of 5 source rows
produced 10 rows). 'replace'/'merge' make re-runs idempotent."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _resource_schema(run_call, name):
    """`run_call` is either a List[DltResource] or a single DltSource (rest_api)."""
    if hasattr(run_call, "resources"):
        return run_call.resources[name].compute_table_schema()
    return next(r for r in run_call if r.name == name).compute_table_schema()


def test_sql_database_write_disposition_reaches_the_real_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders", "primary_key": "id", "write_disposition": "merge"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "merge"


def test_sql_database_defaults_to_append_when_unset(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "append"


def test_rest_api_write_disposition_reaches_the_real_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"name": "orders", "primary_key": "id", "write_disposition": "merge", "endpoint": {"path": "orders"}}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "merge"


def test_filesystem_write_disposition_reaches_the_real_resource(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="filesystem", bucket_url="s3://fake-bucket",
        resources=[{"name": "orders", "file_glob": "orders/*.csv", "primary_key": "id", "write_disposition": "replace"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "replace"


def test_invalid_write_disposition_raises_a_clear_error(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders", "write_disposition": "upsert"}],
    )
    with pytest.raises(ValueError, match="write_disposition must be"):
        component.build_defs(context=None)
