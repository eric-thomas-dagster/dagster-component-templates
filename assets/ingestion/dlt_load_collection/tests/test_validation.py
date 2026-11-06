"""Clear-error validation paths for DltYamlSourceComponent, exercised at
Definitions-build time (before anything reaches dlt itself)."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_empty_resources_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"}, rest_api_resources=[],
    )
    with pytest.raises(ValueError, match="non-empty list"):
        component.build_defs(context=None)


def test_duplicate_resource_name_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "customers", "endpoint": {"path": "customers"}}, {"name": "customers", "endpoint": {"path": "customers2"}}],
    )
    with pytest.raises(ValueError, match="duplicate resource name"):
        component.build_defs(context=None)


def test_resource_missing_name_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"endpoint": {"path": "customers"}}],
    )
    with pytest.raises(ValueError, match="missing the required 'name'"):
        component.build_defs(context=None)


def test_rest_api_resource_missing_endpoint_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "customers"}],
    )
    with pytest.raises(ValueError, match="needs 'endpoint"):
        component.build_defs(context=None)


def test_filesystem_resource_missing_file_glob_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem", filesystem_bucket_url="s3://b",
        filesystem_resources=[{"name": "orders"}],
    )
    with pytest.raises(ValueError, match="needs 'file_glob'"):
        component.build_defs(context=None)


def test_sql_database_missing_connection_string_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database",
        sql_resources=[{"name": "orders"}],
    )
    with pytest.raises(ValueError, match="connection_string"):
        component.build_defs(context=None)


def test_rest_api_missing_client_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api",
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    with pytest.raises(ValueError, match="'client' is required"):
        component.build_defs(context=None)


def test_filesystem_missing_bucket_url_raises(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_resources=[{"name": "orders", "file_glob": "orders/*.csv"}],
    )
    with pytest.raises(ValueError, match="'filesystem_bucket_url' is required"):
        component.build_defs(context=None)


def test_connection_string_env_var_is_accepted_in_place_of_literal(mod, monkeypatch):
    monkeypatch.setenv("MY_CONN", "sqlite:///:memory:")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database", connection_string_env_var="MY_CONN",
        sql_resources=[{"name": "orders"}],
    )
    # Should not raise at build_defs time -- only resource construction (at asset run time) needs it resolved.
    component.build_defs(context=None)


def test_rest_api_resources_set_while_source_type_is_sql_database_raises(mod):
    """Catches the common mistake of setting the wrong resources field for the active source_type."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///:memory:",
        sql_resources=[{"name": "orders"}],
        rest_api_resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    with pytest.raises(ValueError, match="rest_api_resources"):
        component.build_defs(context=None)


def test_destination_credentials_env_var_unset_raises_clearly(mod, monkeypatch):
    monkeypatch.delenv("DLT_LOAD_COLLECTION_TEST_UNSET_VAR", raising=False)
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
        destination="snowflake", destination_credentials_env_var="DLT_LOAD_COLLECTION_TEST_UNSET_VAR",
    )
    with pytest.raises(ValueError, match="DLT_LOAD_COLLECTION_TEST_UNSET_VAR"):
        component._resolve_destination()


def test_connection_string_env_var_unset_raises_clearly(mod, monkeypatch):
    monkeypatch.delenv("DLT_LOAD_COLLECTION_TEST_UNSET_VAR", raising=False)
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database", connection_string_env_var="DLT_LOAD_COLLECTION_TEST_UNSET_VAR",
        sql_resources=[{"name": "orders"}],
    )
    with pytest.raises(ValueError, match="DLT_LOAD_COLLECTION_TEST_UNSET_VAR"):
        component._resolve_source_credentials()


def test_client_auth_token_resolved_to_none_raises_clearly(mod):
    """A "{{ env.VAR_NAME }}" Jinja reference to an unset var resolves to None
    silently (unlike the "{{ env('VAR_NAME') }}" function form, which raises) --
    this is the resulting None reaching the component, not the template itself."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api",
        client={"base_url": "https://x", "auth": {"type": "bearer", "token": None}},
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    with pytest.raises(ValueError, match="client.auth.token"):
        component._build_resources()
