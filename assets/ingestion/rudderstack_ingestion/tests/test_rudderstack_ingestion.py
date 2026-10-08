"""Committed regression tests for RudderStackIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real RudderStack REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the usage-metrics start-date
default, the bearer-auth config shape, the region host switch, and the
per-resource DataFrame combination + metadata shape.
"""
import datetime as dt

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

def test_default_resources_expands_to_tracking_plans_and_audit_logs(mod):
    config_resources = mod._build_resources_config("tracking_plans,audit_logs", None, 100, "day", None)
    names = [r["name"] for r in config_resources]
    assert names == ["tracking_plans", "audit_logs"]


def test_tracking_plans_uses_real_path(mod):
    config_resources = mod._build_resources_config("tracking_plans", None, 100, "day", None)
    assert config_resources[0]["endpoint"]["path"] == "v2/catalog/tracking-plans"


def test_audit_logs_omits_created_after_when_unset(mod):
    config_resources = mod._build_resources_config("audit_logs", None, 100, "day", None)
    params = config_resources[0]["endpoint"]["params"]
    assert params == {"per_page": 100}


def test_audit_logs_includes_created_after_when_set(mod):
    config_resources = mod._build_resources_config("audit_logs", "2026-01-01T00:00:00Z", 50, "day", None)
    params = config_resources[0]["endpoint"]["params"]
    assert params == {"per_page": 50, "created_after": "2026-01-01T00:00:00Z"}


def test_usage_start_date_defaults_to_seven_days_ago(mod):
    config_resources = mod._build_resources_config("usage", None, 100, "day", None)
    params = config_resources[0]["endpoint"]["params"]
    assert params["granularity"] == "day"
    start = dt.date.fromisoformat(params["start"])
    expected = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=7)
    assert abs((start - expected).days) <= 1


def test_usage_start_date_passes_through_when_set(mod):
    config_resources = mod._build_resources_config("usage", None, 100, "month", "2020-01-01")
    params = config_resources[0]["endpoint"]["params"]
    assert params == {"granularity": "month", "start": "2020-01-01"}


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("tracking_plans,not_a_real_resource", None, 100, "day", None)
    assert [r["name"] for r in config_resources] == ["tracking_plans"]


# --- end-to-end: bearer auth + base URL + region switch -----------------------

def test_source_config_uses_bearer_auth_and_us_base_url_by_default(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"tracking_plans": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.rudderstack.com"
    assert config["client"]["auth"] == {"type": "bearer", "token": "tok_abc123"}


def test_eu_region_switches_base_url(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
        region="eu",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"tracking_plans": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["client"]["base_url"] == "https://api.eu.rudderstack.com"


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tracking_plans": pd.DataFrame({"id": [1]}),
            "audit_logs": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "rudderstack_out")
    assert out["resources_requested"] == ["tracking_plans", "audit_logs"]
    assert set(out["resources_loaded"]) == {"tracking_plans", "audit_logs"}
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
        resources="tracking_plans,usage",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tracking_plans": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "usage": pd.DataFrame({"id": [1], "events": [100]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "rudderstack_out")
    assert out["row_count"] == 3
    assert out["rows_tracking_plans"] == 2
    assert out["rows_usage"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
        resources="tracking_plans",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tracking_plans": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "rudderstack_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.RudderStackIngestionComponent(
        asset_name="rudderstack_out",
        service_access_token="tok_abc123",
        resources="tracking_plans",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tracking_plans": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
