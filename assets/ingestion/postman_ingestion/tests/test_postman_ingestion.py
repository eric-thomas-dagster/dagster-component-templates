"""Committed regression tests for PostmanIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Postman REST API, and the follow-up sql_client() query-back -- is
monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the api_key auth config shape, the
real base_url, the wrapped-response data_selector per resource, and the
per-resource DataFrame combination + metadata shape.
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

def test_default_resources_expands_to_requested_dlt_resources(mod):
    config_resources = mod._build_resources_config("collections,workspaces,environments,monitors")
    names = [r["name"] for r in config_resources]
    assert names == ["collections", "workspaces", "environments", "monitors"]


def test_collections_resource_uses_real_path_and_wrapped_selector(mod):
    config_resources = mod._build_resources_config("collections")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "collections"
    # Confirms the real Postman /collections response is wrapped as
    # {"collections": [...]}, NOT a bare array -- data_selector must be
    # the resource name, not "$".
    assert config_resources[0]["endpoint"]["data_selector"] == "collections"


def test_workspaces_resource_uses_real_path_and_wrapped_selector(mod):
    config_resources = mod._build_resources_config("workspaces")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "workspaces"
    assert config_resources[0]["endpoint"]["data_selector"] == "workspaces"


def test_environments_resource_uses_real_path_and_wrapped_selector(mod):
    config_resources = mod._build_resources_config("environments")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "environments"
    assert config_resources[0]["endpoint"]["data_selector"] == "environments"


def test_monitors_resource_uses_real_path_and_wrapped_selector(mod):
    config_resources = mod._build_resources_config("monitors")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "monitors"
    assert config_resources[0]["endpoint"]["data_selector"] == "monitors"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("collections,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["collections"]


def test_empty_resources_yields_no_dlt_resources(mod):
    config_resources = mod._build_resources_config("")
    assert config_resources == []


# --- end-to-end: api_key auth + real base URL + full resource set -----------------

def test_source_config_uses_api_key_auth_and_real_base_url(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"collections": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.postman.com"
    assert config["client"]["auth"] == {
        "type": "api_key",
        "api_key": "PMAK-fake-key-123",
        "name": "x-api-key",
        "location": "header",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "collections": pd.DataFrame({"id": [1]}),
            "workspaces": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "postman_out")
    assert out["resources_requested"] == ["collections", "workspaces"]
    assert set(out["resources_loaded"]) == {"collections", "workspaces"}
    assert out["row_count"] == 2


def test_all_four_resources_requested_metadata(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
        resources="collections,workspaces,environments,monitors",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "collections": pd.DataFrame({"id": [1]}),
            "workspaces": pd.DataFrame({"id": [1]}),
            "environments": pd.DataFrame({"id": [1]}),
            "monitors": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "postman_out")
    assert out["resources_requested"] == ["collections", "workspaces", "environments", "monitors"]
    assert set(out["resources_loaded"]) == {"collections", "workspaces", "environments", "monitors"}
    assert out["row_count"] == 4


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
        resources="collections,environments",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "collections": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "environments": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "postman_out")
    assert out["row_count"] == 3
    assert out["rows_collections"] == 2
    assert out["rows_environments"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
        resources="collections",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"collections": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "postman_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(
        asset_name="postman_out",
        api_key="PMAK-fake-key-123",
        resources="collections",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"collections": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
