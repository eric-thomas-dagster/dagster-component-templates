"""Regression tests for the dlt-based, subsettable-multi-asset rewrite of
ServiceNowIngestionComponent.

Real bugs this covers:
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
  4. Multiple tables: supports two shapes via `asset_shape`. Default ('single',
     matching every other multi-table ingestion component in this repo, e.g.
     salesforce_ingestion/zendesk_ingestion) unions every table into one
     DataFrame under one asset, tagged by a `_table` column. Opt-in
     ('multi_asset', `@multi_asset(can_subset=True)`) gives each table its OWN
     asset keyed `{asset_name}_{table}`, with the dlt resources list passed to
     rest_api_source built from ONLY the selected table(s) -- selecting
     "incident" never even fetches "change_request" from ServiceNow, not just
     "doesn't return it".
  5. `table_name_map` lets a ServiceNow table land under a differently-named
     destination table (dlt's resource `table_name`, distinct from its `name`)
     -- works in both asset shapes.

Credential validation is checked at run time, not at `build_defs` / Definitions
load time (a missing env var at load time would take the whole code location
offline, not just this asset -- the same bug class documented in
workday_ingestion's test_credential_check_is_not_load_time.py).

column_lineage was deliberately dropped in the multi-table rewrite: its
mapping is inherently single-upstream-asset-shaped and doesn't generalize
across this component's now-possibly-heterogeneous per-table assets.
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
        tables="incident",
    )
    defaults.update(overrides)
    return ServiceNowIngestionComponent(**defaults)


def test_build_defs_does_not_raise_with_no_credentials():
    component = _build_component(instance_env_var=None, username_env_var=None, password_env_var=None)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1  # one AssetsDefinition -- the multi_asset group


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
    def __init__(self, df_or_per_table):
        self._df_or_per_table = df_or_per_table

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_query(self, sql, *a, **k):
        df = self._df_or_per_table
        if isinstance(df, dict):
            # Pick the per-table frame whose name appears in the query (e.g. "SELECT * FROM ds.incident").
            match = next((d for name, d in df.items() if name in sql), None)
            df = match if match is not None else pd.DataFrame({"sys_id": []})

        @contextmanager
        def cm():
            yield _Cursor(df)
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

    component = _build_component()  # default asset_shape='single'
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    config = captured[-1]
    assert config["client"]["base_url"] == "https://mycompany.service-now.com/api"
    assert config["client"]["auth"] == {"type": "http_basic", "username": "svc_account", "password": "hunter2"}
    assert config["resources"][0]["endpoint"]["path"] == "now/table/incident"
    paginator = config["client"]["paginator"]
    assert paginator.param_name == "sysparm_offset" and paginator.limit_param == "sysparm_limit"

    df = result.output_for_node(asset_def.op.name)  # single shape -> default output name "result"
    assert len(df) == 4
    assert set(df["_table"]) == {"incident"}


def test_bearer_token_takes_precedence_over_basic_auth(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_TOKEN", "abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(username_env_var=None, password_env_var=None, bearer_token_env_var="SNOW_TOKEN")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

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


def test_blank_or_whitespace_only_tables_raises_a_clear_error():
    component = _build_component(tables="  ,  ")
    with pytest.raises(ValueError, match="'tables' must list at least one"):
        component.build_defs(context=None)


def test_multiple_tables_each_become_their_own_independent_asset(monkeypatch):
    """Two tables -> two separate assets (NOT a union). A full materialize fetches both."""
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    incidents = pd.DataFrame({"sys_id": ["i1", "i2"]})
    change_requests = pd.DataFrame({"sys_id": ["c1"]})
    _patch_dlt(monkeypatch, {"incident": incidents, "change_request": change_requests}, captured)

    component = _build_component(tables="incident,change_request", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_incident"), dg.AssetKey("test_asset_change_request")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["incident", "change_request"]  # full run fetches both

    incident_df = result.output_for_node(asset_def.op.name, output_name="test_asset_incident")
    change_df = result.output_for_node(asset_def.op.name, output_name="test_asset_change_request")
    assert list(incident_df["sys_id"]) == ["i1", "i2"]
    assert list(change_df["sys_id"]) == ["c1"]


def test_materializing_one_table_never_fetches_the_other(monkeypatch):
    """The core requirement: selecting just one table must not even request the others from ServiceNow."""
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    incidents = pd.DataFrame({"sys_id": ["i1", "i2"]})
    change_requests = pd.DataFrame({"sys_id": ["c1"]})
    _patch_dlt(monkeypatch, {"incident": incidents, "change_request": change_requests}, captured)

    component = _build_component(tables="incident,change_request", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_incident")],
    )
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["incident"]  # change_request was never requested from ServiceNow at all

    incident_df = result.output_for_node(asset_def.op.name, output_name="test_asset_incident")
    assert list(incident_df["sys_id"]) == ["i1", "i2"]


def test_asset_shape_defaults_to_single():
    component = _build_component(tables="incident,change_request")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_produces_one_asset_covering_every_table(monkeypatch):
    """asset_shape='single': one asset keyed by asset_name, every table fetched together, no
    per-table subsetting concept -- for when you just want to monitor 'did the pipeline run'."""
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    incidents = pd.DataFrame({"sys_id": ["i1", "i2"]})
    change_requests = pd.DataFrame({"sys_id": ["c1"]})
    _patch_dlt(monkeypatch, {"incident": incidents, "change_request": change_requests}, captured)

    component = _build_component(tables="incident,change_request", asset_shape="single")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["incident", "change_request"]  # always fetches every table together

    df = result.output_for_node(asset_def.op.name)
    assert set(df["sys_id"]) == {"i1", "i2", "c1"}
    assert dict(df.groupby("_table")["sys_id"].count()) == {"incident": 2, "change_request": 1}


def test_table_name_map_overrides_the_destination_table_name(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    # Keyed by DESTINATION name -- the fake _Sql resolves queries by matching table names in the SQL string.
    _patch_dlt(monkeypatch, {"raw_incidents": FRAME}, captured)

    component = _build_component(table_name_map={"incident": "raw_incidents"})  # default asset_shape='single'
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resources = captured[-1]["resources"]
    assert resources[0]["name"] == "incident"
    assert resources[0]["table_name"] == "raw_incidents"

    df = result.output_for_node(asset_def.op.name)
    assert len(df) == 4


def test_table_name_map_overrides_the_destination_table_name_in_multi_asset_mode(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    _patch_dlt(monkeypatch, {"raw_incidents": FRAME}, captured)

    component = _build_component(table_name_map={"incident": "raw_incidents"}, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resources = captured[-1]["resources"]
    assert resources[0]["name"] == "incident"
    assert resources[0]["table_name"] == "raw_incidents"

    df = result.output_for_node(asset_def.op.name, output_name="test_asset_incident")
    assert len(df) == 4


def test_table_name_map_with_an_unknown_table_raises_a_clear_error():
    component = _build_component(tables="incident", table_name_map={"change_request": "cr"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)


def test_partition_date_column_actually_filters_the_dataframe(monkeypatch):
    """Regression test: these fields used to be read into local variables and never applied --
    every partition returned the whole table. Now the row-filter helper is actually wired in
    (checked here for the default 'single' shape)."""
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


def test_partition_date_column_filters_in_multi_asset_mode_too(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(
        asset_shape="multi_asset",
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
    df = result.output_for_node(asset_def.op.name, output_name="test_asset_incident")
    assert set(df["sys_id"]) == {"a", "b"}  # only the 2024-01-01 rows, not all 4
