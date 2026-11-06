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


@pytest.mark.parametrize("source_type,resource", [
    ("sql_database", {"name": "orders", "primary_key": "id"}),
    ("rest_api", {"name": "orders", "primary_key": "id", "endpoint": {"path": "orders"}}),
    ("filesystem", {"name": "orders", "file_glob": "orders/*.csv", "primary_key": "id"}),
])
def test_primary_key_with_no_explicit_write_disposition_infers_merge(mod, monkeypatch, source_type, resource):
    """Real gap: 'append' as the silent default means a declared primary_key does nothing
    to prevent duplicate rows on re-materialization unless write_disposition is also set."""
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    kwargs = dict(asset_name="a", source_type=source_type, resources=[resource])
    if source_type == "sql_database":
        kwargs["connection_string"] = "sqlite:///:memory:"
    elif source_type == "rest_api":
        kwargs["client"] = {"base_url": "https://x"}
    elif source_type == "filesystem":
        kwargs["bucket_url"] = "s3://fake-bucket"
    component = mod.DltLoadCollectionComponent(**kwargs)
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "merge"


def test_explicit_write_disposition_wins_over_the_primary_key_inference(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders", "primary_key": "id", "write_disposition": "append"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "append"


def test_merge_without_primary_key_or_merge_key_raises_a_clear_error(mod):
    """Real gap: dlt's merge strategy with no key silently falls back to appending
    (verified: 2 loads of the same 2 rows produced 4) -- validated at build_defs time instead."""
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders", "write_disposition": "merge"}],
    )
    with pytest.raises(ValueError, match="no natural key to dedupe on"):
        component.build_defs(context=None)


def test_merge_with_only_merge_key_is_accepted_and_wired(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        resources=[{"name": "orders", "write_disposition": "merge", "merge_key": "id"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    schema = _resource_schema(fake_pipeline.run_calls[-1], "orders")
    assert schema["write_disposition"] == "merge"
    assert schema["columns"]["id"]["merge_key"] is True  # dlt stores merge_key as a per-column hint
