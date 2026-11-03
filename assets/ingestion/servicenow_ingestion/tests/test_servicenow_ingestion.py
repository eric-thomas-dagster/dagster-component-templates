"""Regression tests for the dlt-based rewrite of ServiceNowIngestionComponent.

Three real bugs this covers:
  1. The old connector was DataFrame-only, built from raw `requests` calls with
     no `destination`/dlt support -- out of step with the rest of this repo's
     ingestion family. The rewrite goes through dlt's generic REST API source
     (ServiceNow has no official verified dlt source -- confirmed against
     dlthub's own ServiceNow scaffold, which is itself just a rest_api_source
     config, not an installable connector).
  2. The old connector's docstring claimed a `resource_key` fallback for shared
     credentials, but no such field existed anywhere in the class -- setting
     one up with `servicenow_record_upsert`/`servicenow_sensor` (both of which
     DO support it) silently did nothing for ingestion.
  3. `partition_date_column`/`partition_static_column` were declared fields,
     read into local variables, and never actually used to filter the
     returned DataFrame -- every partitioned run returned the WHOLE table,
     not just that partition's rows ("cosmetic partitioning").

Credential validation is checked at run time, not at `build_defs` / Definitions
load time (a missing env var at load time would take the whole code location
offline, not just this asset -- the same bug class documented in
workday_ingestion's test_credential_check_is_not_load_time.py).
"""
import importlib.util
import pathlib
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("servicenow_ingestion_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ServiceNowIngestionComponent = _mod.ServiceNowIngestionComponent

FRAME = pd.DataFrame({
    "sys_id": ["a", "b", "c", "d"],
    "sys_created_on": ["2024-01-01T10:00:00", "2024-01-01T18:00:00", "2024-01-02T10:00:00", "2024-01-02T18:00:00"],
    "short_description": ["one", "two", "three", "four"],
})


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        instance_env_var="SNOW_INSTANCE",
        username_env_var="SNOW_USERNAME",
        password_env_var="SNOW_PASSWORD",
        table="incident",
    )
    defaults.update(overrides)
    return ServiceNowIngestionComponent(**defaults)


def test_build_defs_does_not_raise_with_no_credentials():
    component = _build_component(instance_env_var=None, username_env_var=None, password_env_var=None)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_materializing_with_no_credentials_fails_the_run_not_the_location():
    component = _build_component(instance_env_var=None, username_env_var=None, password_env_var=None)
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "resource_key" in str(exc_info.value)
    assert "instance_env_var" in str(exc_info.value)


class _Cursor:
    def __init__(self, df):
        self._df = df

    def df(self):
        return self._df


class _Sql:
    def __init__(self, df):
        self._df = df

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_query(self, sql, *a, **k):
        @contextmanager
        def cm():
            yield _Cursor(self._df)
        return cm()


class _Pipeline:
    def __init__(self, df, **kw):
        self._df = df

    def run(self, *a, **k):
        return "load-info"

    def sql_client(self):
        return _Sql(self._df)


def _patch_dlt(monkeypatch, df, captured_configs):
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline(df, **kw))
    import dlt.sources.rest_api as _rest_api

    def _fake_rest_api_source(config):
        captured_configs.append(config)
        return object()

    monkeypatch.setattr(_rest_api, "rest_api_source", _fake_rest_api_source)


def test_env_var_path_builds_offset_paginator_and_returns_dataframe(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    config = captured[-1]
    assert config["client"]["base_url"] == "https://mycompany.service-now.com/api"
    assert config["client"]["auth"] == {"type": "http_basic", "username": "svc_account", "password": "hunter2"}
    assert config["resources"][0]["endpoint"]["path"] == "now/table/incident"
    paginator = config["client"]["paginator"]
    assert paginator.param_name == "sysparm_offset" and paginator.limit_param == "sysparm_limit"

    df = result.output_for_node(asset_def.op.name)
    assert len(df) == 4


def test_bearer_token_takes_precedence_over_basic_auth(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_TOKEN", "abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(username_env_var=None, password_env_var=None, bearer_token_env_var="SNOW_TOKEN")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert captured[-1]["client"]["auth"] == {"type": "bearer", "token": "abc123"}


def test_resource_key_path_reads_credentials_from_the_shared_resource(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(instance_env_var=None, username_env_var=None, password_env_var=None, resource_key="snow")
    asset_def = component.build_defs(context=None).assets[0]

    class _FakeServiceNowResource:
        instance = "fromresource"
        username = "res_user"
        password = "res_pass"
        bearer_token = None
        verify_ssl = True

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager(), "snow": _FakeServiceNowResource()},
    )
    assert result.success
    assert captured[-1]["client"]["base_url"] == "https://fromresource.service-now.com/api"
    assert captured[-1]["client"]["auth"] == {"type": "http_basic", "username": "res_user", "password": "res_pass"}


def test_partition_date_column_actually_filters_the_dataframe(monkeypatch):
    """Regression test: these fields used to be read into local variables and never applied --
    every partition returned the whole table. Now the row-filter helper is actually wired in."""
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="sys_created_on",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def],
        partition_key="2024-01-01",
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    df = result.output_for_node(asset_def.op.name)
    assert set(df["sys_id"]) == {"a", "b"}  # only the 2024-01-01 rows, not all 4
