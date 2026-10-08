"""Committed regression tests for LogRocketIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real LogRocket REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the `session_exports` `date` param
conversion (`_iso_date_to_epoch_ms`), the custom 'token' auth config shape,
and the per-resource DataFrame combination + metadata shape.
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


# --- _build_resources_config / _iso_date_to_epoch_ms: pure logic, no mocking needed ----

def test_default_resources_expands_to_two_dlt_resources(mod):
    config_resources = mod._build_resources_config("audit_logs,session_exports", None)
    names = [r["name"] for r in config_resources]
    assert names == ["audit_logs", "session_exports"]


def test_audit_logs_resource_uses_real_path_and_cursor_paginator(mod):
    config_resources = mod._build_resources_config("audit_logs", None)
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "audit/logs/"
    assert endpoint["data_selector"] == "logs"
    assert endpoint["paginator"] == {
        "type": "cursor",
        "cursor_path": "cursor",
        "cursor_param": "cursor",
    }
    # No params by default -- cursor/limit are handled by the paginator,
    # not sent as a fixed query param.
    assert "params" not in endpoint


def test_session_exports_resource_uses_real_path_and_selector(mod):
    config_resources = mod._build_resources_config("session_exports", None)
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "data-export/"
    assert endpoint["data_selector"] == "sessions"
    assert "params" not in endpoint  # no export_start_date set


def test_session_exports_date_param_converts_iso_to_epoch_ms(mod):
    config_resources = mod._build_resources_config("session_exports", "2026-01-01")
    params = config_resources[0]["endpoint"]["params"]
    # 2026-01-01T00:00:00Z in epoch ms
    import datetime as dt
    expected = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    assert params == {"date": expected}


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("audit_logs,not_a_real_resource", None)
    assert [r["name"] for r in config_resources] == ["audit_logs"]


def test_audit_logs_not_requested_means_only_session_exports(mod):
    config_resources = mod._build_resources_config("session_exports", None)
    assert all(r["name"] != "audit_logs" for r in config_resources)


# --- end-to-end: custom token auth + real base URL + full resource set ---------------

def test_source_config_uses_token_auth_scheme_and_real_base_url(mod, monkeypatch):
    component = mod.LogRocketIngestionComponent(
        asset_name="logrocket_out",
        org_id="org_123",
        app_id="app_456",
        api_key="key_abc",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"audit_logs": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.logrocket.com/v1/orgs/org_123/apps/app_456"
    # Confirms the component does NOT use a Bearer scheme -- LogRocket's real
    # API requires `Authorization: token <api_key>`, built here via dlt's
    # "api_key" auth type targeting the Authorization header directly.
    assert config["client"]["auth"] == {
        "type": "api_key",
        "name": "Authorization",
        "api_key": "token key_abc",
        "location": "header",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.LogRocketIngestionComponent(
        asset_name="logrocket_out",
        org_id="org_123",
        app_id="app_456",
        api_key="key_abc",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "audit_logs": pd.DataFrame({"id": [1]}),
            "session_exports": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "logrocket_out")
    assert out["resources_requested"] == ["audit_logs", "session_exports"]
    assert set(out["resources_loaded"]) == {"audit_logs", "session_exports"}
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -------------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.LogRocketIngestionComponent(
        asset_name="logrocket_out",
        org_id="org_123",
        app_id="app_456",
        api_key="key_abc",
        resources="audit_logs,session_exports",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "audit_logs": pd.DataFrame({"id": [1, 2], "action": ["login", "export"]}),
            "session_exports": pd.DataFrame({"id": [1], "url": ["https://example.com/f1"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "logrocket_out")
    assert out["row_count"] == 3
    assert out["rows_audit_logs"] == 2
    assert out["rows_session_exports"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.LogRocketIngestionComponent(
        asset_name="logrocket_out",
        org_id="org_123",
        app_id="app_456",
        api_key="key_abc",
        resources="audit_logs",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"audit_logs": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "logrocket_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.LogRocketIngestionComponent(
        asset_name="logrocket_out",
        org_id="org_123",
        app_id="app_456",
        api_key="key_abc",
        resources="audit_logs",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"audit_logs": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
