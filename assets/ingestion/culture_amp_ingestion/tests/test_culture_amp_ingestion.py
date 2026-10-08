"""Committed regression tests for CultureAmpIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Culture Amp REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the OAuth2 client-credentials auth
config shape (`_build_auth_config`), and the per-resource DataFrame
combination + metadata shape.
"""
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, raise_on_error=True):
    import dagster as dg

    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_three_resources(mod):
    config_resources = mod._build_resources_config("employees,performance_cycles,manager_reviews")
    names = [r["name"] for r in config_resources]
    assert names == ["employees", "performance_cycles", "manager_reviews"]


def test_employees_resource_uses_real_path_and_data_selector(mod):
    config_resources = mod._build_resources_config("employees")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "employees"
    assert config_resources[0]["endpoint"]["path"] == "employees"
    assert config_resources[0]["endpoint"]["data_selector"] == "data"


def test_performance_cycles_resource_uses_hyphenated_real_path(mod):
    # The component's resource name is snake_case (performance_cycles) but
    # the real Culture Amp path is hyphenated (performance-cycles) -- this
    # is the exact detail a naive name->path mapping would get wrong.
    config_resources = mod._build_resources_config("performance_cycles")
    assert config_resources[0]["endpoint"]["path"] == "performance-cycles"


def test_manager_reviews_resource_uses_hyphenated_real_path(mod):
    config_resources = mod._build_resources_config("manager_reviews")
    assert config_resources[0]["endpoint"]["path"] == "manager-reviews"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("employees,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["employees"]


def test_subset_of_resources_preserves_requested_order(mod):
    config_resources = mod._build_resources_config("manager_reviews,employees")
    assert [r["name"] for r in config_resources] == ["manager_reviews", "employees"]


# --- _build_auth_config: pure logic, no mocking needed -----------------------------

def test_auth_config_uses_oauth2_client_credentials_and_real_token_url(mod):
    auth = mod._build_auth_config("client_abc", "secret_xyz", "employees-read,surveys-read")
    assert auth["type"] == "oauth2_client_credentials"
    assert auth["access_token_url"] == "https://api.cultureamp.com/v1/oauth2/token"
    assert auth["client_id"] == "client_abc"
    assert auth["client_secret"] == "secret_xyz"


def test_auth_config_joins_scopes_as_space_separated_string(mod):
    auth = mod._build_auth_config("client_abc", "secret_xyz", "employees-read, surveys-read")
    assert auth["access_token_request_data"] == {"scope": "employees-read surveys-read"}


def test_auth_config_single_scope(mod):
    auth = mod._build_auth_config("client_abc", "secret_xyz", "employees-read")
    assert auth["access_token_request_data"] == {"scope": "employees-read"}


# --- end-to-end: auth config + base URL + full resource set -----------------------

def test_source_config_uses_real_base_url_and_oauth_config(mod, monkeypatch):
    component = mod.CultureAmpIngestionComponent(
        asset_name="culture_amp_out",
        client_id="client_abc",
        client_secret="secret_xyz",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"employees": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.cultureamp.com/v1"
    assert config["client"]["auth"]["type"] == "oauth2_client_credentials"
    assert config["client"]["auth"]["client_id"] == "client_abc"
    assert config["client"]["auth"]["client_secret"] == "secret_xyz"


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.CultureAmpIngestionComponent(
        asset_name="culture_amp_out",
        client_id="client_abc",
        client_secret="secret_xyz",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "employees": pd.DataFrame({"id": [1]}),
            "performance_cycles": pd.DataFrame({"id": [1]}),
            "manager_reviews": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "culture_amp_out")
    assert out["resources_requested"] == ["employees", "performance_cycles", "manager_reviews"]
    assert set(out["resources_loaded"]) == {"employees", "performance_cycles", "manager_reviews"}
    assert out["row_count"] == 3


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.CultureAmpIngestionComponent(
        asset_name="culture_amp_out",
        client_id="client_abc",
        client_secret="secret_xyz",
        resources="employees,manager_reviews",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "employees": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "manager_reviews": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "culture_amp_out")
    assert out["row_count"] == 3
    assert out["rows_employees"] == 2
    assert out["rows_manager_reviews"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.CultureAmpIngestionComponent(
        asset_name="culture_amp_out",
        client_id="client_abc",
        client_secret="secret_xyz",
        resources="employees",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"employees": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "culture_amp_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.CultureAmpIngestionComponent(
        asset_name="culture_amp_out",
        client_id="client_abc",
        client_secret="secret_xyz",
        resources="employees",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"employees": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
