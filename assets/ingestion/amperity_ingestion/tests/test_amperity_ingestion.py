"""Committed regression tests for AmperityIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Amperity REST API, and the follow-up sql_client() query-back -- is
monkeypatched wholesale via `install_fake_dlt` (see conftest.py). Everything
the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the audit-events query-param
filtering, the tenant-subdomain bearer-auth base URL, and the per-resource
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

def test_default_resources_expands_to_audit_events_and_segments(mod):
    config_resources = mod._build_resources_config("audit_events,segments", None, None, 100)
    names = [r["name"] for r in config_resources]
    assert names == ["audit_events", "segments"]


def test_audit_events_uses_real_path(mod):
    config_resources = mod._build_resources_config("audit_events", None, None, 100)
    assert config_resources[0]["endpoint"]["path"] == "api/audit-events"


def test_audit_events_params_omit_unset_date_filters(mod):
    config_resources = mod._build_resources_config("audit_events", None, None, 250)
    params = config_resources[0]["endpoint"]["params"]
    assert params == {"limit": 250}


def test_audit_events_params_include_date_filters_when_set(mod):
    config_resources = mod._build_resources_config(
        "audit_events", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", 100,
    )
    params = config_resources[0]["endpoint"]["params"]
    assert params == {
        "limit": 100,
        "happened_from": "2026-01-01T00:00:00Z",
        "happened_to": "2026-02-01T00:00:00Z",
    }


def test_all_four_resources_recognized(mod):
    config_resources = mod._build_resources_config(
        "audit_events,campaigns,segments,ingest_jobs", None, None, 100,
    )
    names = [r["name"] for r in config_resources]
    assert names == ["audit_events", "campaigns", "segments", "ingest_jobs"]
    paths = [r["endpoint"]["path"] for r in config_resources]
    assert paths == ["api/audit-events", "api/campaigns", "api/segments", "api/ingest/jobs"]


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("segments,not_a_real_resource", None, None, 100)
    assert [r["name"] for r in config_resources] == ["segments"]


# --- end-to-end: bearer auth + tenant-subdomain base URL -----------------------

def test_source_config_uses_bearer_auth_and_tenant_subdomain(mod, monkeypatch):
    component = mod.AmperityIngestionComponent(
        asset_name="amperity_out",
        tenant="mycompany",
        access_token="jwt_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"audit_events": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://mycompany.amperity.com"
    assert config["client"]["auth"] == {"type": "bearer", "token": "jwt_abc123"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.AmperityIngestionComponent(
        asset_name="amperity_out",
        tenant="mycompany",
        access_token="jwt_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "audit_events": pd.DataFrame({"id": [1]}),
            "segments": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "amperity_out")
    assert out["resources_requested"] == ["audit_events", "segments"]
    assert set(out["resources_loaded"]) == {"audit_events", "segments"}
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.AmperityIngestionComponent(
        asset_name="amperity_out",
        tenant="mycompany",
        access_token="jwt_abc123",
        resources="campaigns,ingest_jobs",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "campaigns": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "ingest_jobs": pd.DataFrame({"id": [1], "status": ["complete"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "amperity_out")
    assert out["row_count"] == 3
    assert out["rows_campaigns"] == 2
    assert out["rows_ingest_jobs"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.AmperityIngestionComponent(
        asset_name="amperity_out",
        tenant="mycompany",
        access_token="jwt_abc123",
        resources="segments",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"segments": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "amperity_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.AmperityIngestionComponent(
        asset_name="amperity_out",
        tenant="mycompany",
        access_token="jwt_abc123",
        resources="segments",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"segments": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []
