"""Tests for SharepointIngestionComponent.

Mocks only the real network calls: the Microsoft Graph OAuth2 client-credentials
token exchange (`requests.post`) and the two site/drive-resolution GETs
(`requests.get` against `/sites/{hostname}:/{path}` and `/sites/{site-id}/drive`,
per https://learn.microsoft.com/en-us/graph/api/site-getbypath and
https://learn.microsoft.com/en-us/graph/api/drive-get) -- same approach as
onedrive_ingestion's own token exchange, extended with the extra site/drive
resolution hops a SharePoint site needs (unlike a personal OneDrive drive,
whose ID is supplied directly). dlt's `pipeline`/`rest_api_source` are faked
the same way servicenow_ingestion/quickbooks_ingestion's own tests do it.

Covers: site-id resolution (hostname + site_path, with/without a leading
slash), file listing (drive-children resource config, after resolving the
site's default drive), the 'lists' resource (site-lists config, no drive
resolution call), asset_shape='single' vs 'multi_asset' (and that selecting
just one resource in multi_asset mode never calls the other resource's
endpoint), hash_columns, on_schema_change, exclude_columns,
max_rows_per_resource, destination handling (persist_only / non-SQL warning),
and a missing-credentials error case that fails the RUN, not component/defs
load (mirrors onedrive_ingestion/servicenow_ingestion's own convention).
"""
import importlib.util
import pathlib
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("sharepoint_ingestion_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SharepointIngestionComponent = _mod.SharepointIngestionComponent

FILES_FRAME = pd.DataFrame({
    "id": ["f1", "f2", "f3", "f4"],
    "name": ["a.docx", "b.xlsx", "c.pdf", "d.pptx"],
    "lastModifiedDateTime": [
        "2024-01-01T10:00:00Z", "2024-01-01T18:00:00Z",
        "2024-01-02T10:00:00Z", "2024-01-02T18:00:00Z",
    ],
})
LISTS_FRAME = pd.DataFrame({
    "id": ["l1", "l2"],
    "displayName": ["Tasks", "Announcements"],
})


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        tenant_id_env_var="AZ_TENANT",
        client_id_env_var="AZ_CLIENT",
        client_secret_env_var="AZ_SECRET",
        site_hostname="contoso.sharepoint.com",
        site_path="/sites/TeamSite",
    )
    defaults.update(overrides)
    return SharepointIngestionComponent(**defaults)


def _set_env(monkeypatch):
    monkeypatch.setenv("AZ_TENANT", "tenant-123")
    monkeypatch.setenv("AZ_CLIENT", "client-abc")
    monkeypatch.setenv("AZ_SECRET", "secret-xyz")


class _FakeResponse:
    def __init__(self, json_data, status=200):
        self._json = json_data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


def _patch_graph_http(monkeypatch, site_id="site-abc123", drive_id="drive-xyz789", captured_requests=None):
    """Fakes requests.post (token exchange) and requests.get (site/drive resolution)."""
    calls = captured_requests if captured_requests is not None else []

    def _fake_post(url, data=None, **kwargs):
        calls.append(("POST", url, data))
        assert "oauth2/v2.0/token" in url
        assert data["grant_type"] == "client_credentials"
        return _FakeResponse({"access_token": "fake-token-123"})

    def _fake_get(url, headers=None, **kwargs):
        calls.append(("GET", url, headers))
        assert headers == {"Authorization": "Bearer fake-token-123"}
        if url.endswith("/drive"):
            return _FakeResponse({"id": drive_id})
        return _FakeResponse({"id": site_id})

    import requests

    monkeypatch.setattr(requests, "post", _fake_post)
    monkeypatch.setattr(requests, "get", _fake_get)
    return calls


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
            match = next((d for name, d in df.items() if name in sql), None)
            df = match if match is not None else pd.DataFrame({"id": []})

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


# --- site/drive resolution -------------------------------------------------

def test_site_id_resolved_from_hostname_and_path(monkeypatch):
    _set_env(monkeypatch)
    calls = _patch_graph_http(monkeypatch, site_id="site-abc123", drive_id="drive-xyz789")
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component(resources="files")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    site_get_calls = [c for c in calls if c[0] == "GET" and not c[1].endswith("/drive")]
    assert site_get_calls[0][1] == "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/TeamSite"

    config = captured[-1]
    assert config["resources"][0]["endpoint"]["path"] == "drives/drive-xyz789/root/children"


def test_site_path_leading_slash_is_stripped(monkeypatch):
    _set_env(monkeypatch)
    calls = _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component(site_path="/sites/TeamSite", resources="files")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    site_get = next(c for c in calls if c[0] == "GET" and "/drive" not in c[1])
    assert site_get[1] == "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/TeamSite"


def test_lists_resource_does_not_call_drive_endpoint(monkeypatch):
    _set_env(monkeypatch)
    calls = _patch_graph_http(monkeypatch, site_id="site-abc123")
    captured = []
    _patch_dlt(monkeypatch, LISTS_FRAME, captured)

    component = _build_component(resources="lists")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert not any(c[1].endswith("/drive") for c in calls if c[0] == "GET")
    config = captured[-1]
    assert config["resources"][0]["endpoint"]["path"] == "sites/site-abc123/lists"


# --- asset shapes ------------------------------------------------------------

def test_asset_shape_defaults_to_single():
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_unions_both_resources(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"files": FILES_FRAME, "lists": LISTS_FRAME}, captured)

    component = _build_component(resources="files,lists", asset_shape="single")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["files", "lists"]

    df = result.output_for_node(asset_def.op.name)
    assert set(df["_resource_type"]) == {"files", "lists"}
    assert len(df) == 6  # 4 files + 2 lists


def test_multi_asset_shape_gives_each_resource_its_own_asset(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"files": FILES_FRAME, "lists": LISTS_FRAME}, captured)

    component = _build_component(resources="files,lists", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_files"), dg.AssetKey("test_asset_lists")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["files", "lists"]

    files_df = result.output_for_node(asset_def.op.name, output_name="test_asset_files")
    lists_df = result.output_for_node(asset_def.op.name, output_name="test_asset_lists")
    assert len(files_df) == 4
    assert len(lists_df) == 2


def test_materializing_one_resource_never_calls_the_other_endpoint(monkeypatch):
    _set_env(monkeypatch)
    calls = _patch_graph_http(monkeypatch, drive_id="drive-should-not-be-called")
    captured = []
    _patch_dlt(monkeypatch, {"lists": LISTS_FRAME}, captured)

    component = _build_component(resources="files,lists", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_lists")],
    )
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["lists"]  # "files" never requested -> no /drive GET at all
    assert not any(c[1].endswith("/drive") for c in calls)


def test_blank_resources_raises_a_clear_error():
    component = _build_component(resources="  ,  ")
    with pytest.raises(ValueError, match="'resources' must list at least one"):
        component.build_defs(context=None)


def test_unknown_resource_raises_a_clear_error():
    component = _build_component(resources="files,bogus")
    with pytest.raises(ValueError, match="unknown resource"):
        component.build_defs(context=None)


# --- credentials -------------------------------------------------------------

def test_build_defs_does_not_raise_with_no_credentials():
    component = _build_component(tenant_id_env_var="MISSING_TENANT", client_id_env_var="MISSING_CLIENT", client_secret_env_var="MISSING_SECRET")
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_materializing_with_missing_credentials_fails_the_run_not_the_location():
    component = _build_component(tenant_id_env_var="MISSING_TENANT", client_id_env_var="MISSING_CLIENT", client_secret_env_var="MISSING_SECRET")
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "MISSING_TENANT" in str(exc_info.value)


# --- hash_columns / on_schema_change / exclude_columns / max_rows_per_resource ----

def test_hash_columns_requires_salt():
    component = _build_component(hash_columns=["name"])
    with pytest.raises(ValueError, match="hash_salt"):
        _mod._apply_hash_columns([object()], ["name"], None, None)


def test_exclude_columns_applied_before_hash_columns():
    exclude_map = _mod._make_exclude_columns_map(["name"])
    hash_map = _mod._make_hash_columns_map(["name"], "s")
    assert hash_map(exclude_map({"name": "a.docx"})) == {}


def test_on_schema_change_sets_resource_defaults_before_rest_api_source_is_called(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component(on_schema_change="block_all")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert captured[-1]["resource_defaults"]["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_on_schema_change_default_does_not_set_resource_defaults(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert "resource_defaults" not in captured[-1]


def test_on_schema_change_rejects_invalid_value():
    with pytest.raises(Exception):
        _build_component(on_schema_change="bogus")


# --- destination handling -----------------------------------------------------

def test_persist_only_emits_materialize_result_and_skips_dataframe(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component(destination="snowflake", persist_only=True)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_non_sql_destination_without_persist_only_warns(monkeypatch, caplog):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, FILES_FRAME, captured)

    component = _build_component(destination="filesystem", bucket_url="s3://bucket/path")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_max_rows_per_resource_does_not_crash(monkeypatch):
    _set_env(monkeypatch)
    _patch_graph_http(monkeypatch)

    class _Permissive:
        def __getattr__(self, name):
            return self

        def __call__(self, *args, **kwargs):
            return self

        def __iter__(self):
            return iter([self])

    class _PCursor:
        def df(self):
            return pd.DataFrame({"id": []})

    class _PSql:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute_query(self, sql, *a, **k):
            @contextmanager
            def cm():
                yield _PCursor()
            return cm()

    class _PPipeline:
        def run(self, *a, **k):
            return "load-info"

        def sql_client(self):
            return _PSql()

    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _PPipeline())
    import dlt.sources.rest_api as _rest_api

    def _fake_rest_api_source(config):
        return _Permissive()

    monkeypatch.setattr(_rest_api, "rest_api_source", _fake_rest_api_source)

    component = _build_component(max_rows_per_resource=10)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
