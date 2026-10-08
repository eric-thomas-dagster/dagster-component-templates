"""Committed regression tests for ScaleAIIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Scale AI REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the http_basic auth config shape
with a blank password, the `tasks` resource's `data_selector: "docs"`
unwrap, and the per-resource DataFrame combination + metadata shape.
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
    config_resources = mod._build_resources_config("tasks,batches,projects")
    names = [r["name"] for r in config_resources]
    assert names == ["tasks", "batches", "projects"]


def test_tasks_resource_uses_docs_data_selector(mod):
    config_resources = mod._build_resources_config("tasks")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "tasks"
    # Confirms the per-item wrapper-key unwrap: scale.com's own docs and
    # dltHub's Context marketplace page both state tasks records are
    # nested under a "docs" key, not returned as a flat array.
    assert config_resources[0]["endpoint"]["data_selector"] == "docs"


def test_batches_resource_has_no_docs_wrapper(mod):
    config_resources = mod._build_resources_config("batches")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "batches"
    assert config_resources[0]["endpoint"]["path"] == "batches"
    assert config_resources[0]["endpoint"]["data_selector"] == "$"


def test_projects_resource_has_no_docs_wrapper(mod):
    config_resources = mod._build_resources_config("projects")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "projects"
    assert config_resources[0]["endpoint"]["path"] == "projects"
    assert config_resources[0]["endpoint"]["data_selector"] == "$"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("tasks,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["tasks"]


def test_empty_resources_string_yields_no_resources(mod):
    assert mod._build_resources_config("") == []


# --- end-to-end: http_basic auth (blank password) + real base URL -----------------

def test_source_config_uses_http_basic_auth_with_blank_password_and_real_base_url(mod, monkeypatch):
    component = mod.ScaleAIIngestionComponent(
        asset_name="scale_ai_out",
        api_key="live_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.scale.com/v1"
    # Scale AI's docs explicitly specify a BLANK/EMPTY password (not a
    # literal placeholder character like chargify_ingestion's "x") --
    # confirmed against scale.com/docs/api-reference/authentication.
    assert config["client"]["auth"] == {
        "type": "http_basic",
        "username": "live_abc123",
        "password": "",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.ScaleAIIngestionComponent(
        asset_name="scale_ai_out",
        api_key="live_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tasks": pd.DataFrame({"id": [1]}),
            "batches": pd.DataFrame({"id": [1]}),
            "projects": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "scale_ai_out")
    assert out["resources_requested"] == ["tasks", "batches", "projects"]
    assert set(out["resources_loaded"]) == {"tasks", "batches", "projects"}
    assert out["row_count"] == 3


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.ScaleAIIngestionComponent(
        asset_name="scale_ai_out",
        api_key="live_abc123",
        resources="tasks,batches",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tasks": pd.DataFrame({"id": [1, 2], "status": ["completed", "pending"]}),
            "batches": pd.DataFrame({"id": [1], "name": ["batch_1"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "scale_ai_out")
    assert out["row_count"] == 3
    assert out["rows_tasks"] == 2
    assert out["rows_batches"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.ScaleAIIngestionComponent(
        asset_name="scale_ai_out",
        api_key="live_abc123",
        resources="tasks",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "scale_ai_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.ScaleAIIngestionComponent(
        asset_name="scale_ai_out",
        api_key="live_abc123",
        resources="tasks",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
