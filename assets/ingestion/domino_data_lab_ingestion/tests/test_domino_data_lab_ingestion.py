"""Committed regression tests for DominoDataLabIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Domino Data Lab REST API, and the follow-up sql_client()
query-back -- is monkeypatched wholesale via `install_fake_dlt` (see
conftest.py). Everything the component actually owns is exercised for
real: resource-config construction (`_build_resources_config`), the
deployment_url -> base_url normalization (with and without a trailing
slash), the X-Domino-Api-Key auth config shape, and the per-resource
DataFrame combination + metadata shape.
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

def test_default_resources_expands_to_three_dlt_resources(mod):
    config_resources = mod._build_resources_config("users,projects,environments")
    names = [r["name"] for r in config_resources]
    assert names == ["users", "projects", "environments"]


def test_users_resource_uses_real_path_and_envelope_selector(mod):
    config_resources = mod._build_resources_config("users")
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "api/users/v1/users"
    # Response is an envelope {"users": [...], "metadata": {...}}, not a
    # bare array -- data_selector must point at the "users" key.
    assert endpoint["data_selector"] == "users"


def test_projects_resource_uses_real_path_and_envelope_selector(mod):
    config_resources = mod._build_resources_config("projects")
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "api/projects/beta/projects"
    assert endpoint["data_selector"] == "projects"
    assert endpoint["params"] == {"limit": 100, "offset": 0}


def test_environments_resource_uses_real_path_and_envelope_selector(mod):
    config_resources = mod._build_resources_config("environments")
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "api/environments/beta/environments"
    assert endpoint["data_selector"] == "environments"
    assert endpoint["params"] == {"limit": 100, "offset": 0}


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("users,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["users"]


def test_empty_resources_yields_no_dlt_resources(mod):
    assert mod._build_resources_config("") == []


# --- base_url normalization: pure logic (exercised via the real dlt-config path) ----

def test_base_url_strips_trailing_slash(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com/",
        api_key="key_abc123",
        resources="users",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["client"]["base_url"] == "https://domino.mycompany.com"


def test_base_url_unchanged_without_trailing_slash(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
        resources="users",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["client"]["base_url"] == "https://domino.mycompany.com"


# --- end-to-end: X-Domino-Api-Key auth shape + full resource set --------------------

def test_source_config_uses_domino_api_key_auth_shape(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={
            "users": pd.DataFrame({"id": [1]}),
            "projects": pd.DataFrame({"id": [1]}),
            "environments": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["auth"] == {
        "type": "api_key",
        "api_key": "key_abc123",
        "name": "X-Domino-Api-Key",
        "location": "header",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "users": pd.DataFrame({"id": [1]}),
            "projects": pd.DataFrame({"id": [1]}),
            "environments": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "domino_out")
    assert out["resources_requested"] == ["users", "projects", "environments"]
    assert set(out["resources_loaded"]) == {"users", "projects", "environments"}
    assert out["row_count"] == 3


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
        resources="users,projects",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "users": pd.DataFrame({"id": [1, 2], "userName": ["a", "b"]}),
            "projects": pd.DataFrame({"id": [1], "name": ["proj1"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "domino_out")
    assert out["row_count"] == 3
    assert out["rows_users"] == 2
    assert out["rows_projects"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
        resources="users",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "domino_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.DominoDataLabIngestionComponent(
        asset_name="domino_out",
        deployment_url="https://domino.mycompany.com",
        api_key="key_abc123",
        resources="users",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
