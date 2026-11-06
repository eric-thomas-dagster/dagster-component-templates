"""Clear-error validation paths for DltLoadCollectionComponent, exercised at
Definitions-build time (before anything reaches dlt itself)."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_empty_resources_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"}, resources=[],
    )
    with pytest.raises(ValueError, match="non-empty list"):
        component.build_defs(context=None)


def test_duplicate_resource_name_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}, {"name": "customers", "endpoint": {"path": "customers2"}}],
    )
    with pytest.raises(ValueError, match="duplicate resource name"):
        component.build_defs(context=None)


def test_resource_missing_name_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"endpoint": {"path": "customers"}}],
    )
    with pytest.raises(ValueError, match="missing the required 'name'"):
        component.build_defs(context=None)


def test_rest_api_resource_missing_endpoint_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"name": "customers"}],
    )
    with pytest.raises(ValueError, match="needs 'endpoint"):
        component.build_defs(context=None)


def test_filesystem_resource_missing_file_glob_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="filesystem", bucket_url="s3://b",
        resources=[{"name": "orders"}],
    )
    with pytest.raises(ValueError, match="needs 'file_glob'"):
        component.build_defs(context=None)


def test_sql_database_missing_connection_string_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database",
        resources=[{"name": "orders"}],
    )
    with pytest.raises(ValueError, match="connection_string"):
        component.build_defs(context=None)


def test_rest_api_missing_client_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api",
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    with pytest.raises(ValueError, match="'client' is required"):
        component.build_defs(context=None)


def test_filesystem_missing_bucket_url_raises(mod):
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="filesystem",
        resources=[{"name": "orders", "file_glob": "orders/*.csv"}],
    )
    with pytest.raises(ValueError, match="'bucket_url' is required"):
        component.build_defs(context=None)


def test_connection_string_env_var_is_accepted_in_place_of_literal(mod, monkeypatch):
    monkeypatch.setenv("MY_CONN", "sqlite:///:memory:")
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database", connection_string_env_var="MY_CONN",
        resources=[{"name": "orders"}],
    )
    # Should not raise at build_defs time -- only resource construction (at asset run time) needs it resolved.
    component.build_defs(context=None)
