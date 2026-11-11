"""Tests for GitlabIngestionComponent.

GitLab has no official dlt-maintained verified-source package (confirmed against
dlthub's own verified-sources catalog, https://dlthub.com/docs/dlt-ecosystem/
verified-sources/ -- GitHub, Salesforce, Zendesk, ServiceNow, etc. are listed; GitLab
is not), so this goes through dlt's generic REST API source
(dlt.sources.rest_api.rest_api_source), same pattern as servicenow_ingestion --
this component's closest structural precedent (also multi-table, also no official
source, also supports asset_shape='single'/'multi_asset').

Real API details this component is built against (docs.gitlab.com/ee/api/rest/,
verified 2026-11):
  - auth: 'PRIVATE-TOKEN' header (personal/project/group access token)
  - pagination: RFC-5988 Link header with rel="next" -- same mechanism GitHub uses,
    handled generically here via dlt's HeaderLinkPaginator (see
    test_gitlab_pagination.py for a real-pagination, HTTP-layer-only-mocked test)
  - issues/merge_requests/commits are project-scoped (GET /projects/:id/...) and
    require project_id; projects can be scoped to group_id or left unscoped

These tests fake dlt.pipeline + dlt.sources.rest_api.rest_api_source (same pattern
as servicenow_ingestion's test suite) to check the REST API config dict built by
the component, and the dataflow around it (asset shapes, subset selection,
table_name_map, hash/exclude/max-rows options, on_schema_change). Credential
validation (missing access token) is checked at materialize time, not at
build_defs/Definitions-load time -- a missing env var at load time would take the
whole code location offline, not just this asset (same bug class documented in
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
_spec = importlib.util.spec_from_file_location("gitlab_ingestion_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

GitlabIngestionComponent = _mod.GitlabIngestionComponent

FRAME = pd.DataFrame({
    "iid": [1, 2, 3, 4],
    "updated_at": ["2024-01-01T10:00:00Z", "2024-01-01T18:00:00Z", "2024-01-02T10:00:00Z", "2024-01-02T18:00:00Z"],
    "title": ["one", "two", "three", "four"],
})


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        access_token_env_var="GL_TOKEN",
        project_id="my-group/proj",
        tables="issues",
    )
    defaults.update(overrides)
    return GitlabIngestionComponent(**defaults)


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
            df = match if match is not None else pd.DataFrame({"iid": []})

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


# --- build_defs / credential-timing -----------------------------------------


def test_build_defs_does_not_raise_with_no_token():
    component = _build_component(access_token_env_var=None)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_missing_token_env_var_fails_the_run_not_the_location():
    """access_token_env_var is set but the env var itself is unset -- this must fail at
    materialize time (a clear KeyError naming the var), not at Definitions-load time."""
    component = _build_component(access_token_env_var="GL_TOKEN_NOT_SET")
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "GL_TOKEN_NOT_SET" in str(exc_info.value)


def test_blank_or_whitespace_only_tables_raises_a_clear_error():
    component = _build_component(tables="  ,  ")
    with pytest.raises(ValueError, match="'tables' must list at least one"):
        component.build_defs(context=None)


def test_unknown_table_raises_a_clear_error():
    component = _build_component(tables="issues,bogus_table")
    with pytest.raises(ValueError, match="unknown entries"):
        component.build_defs(context=None)


def test_project_scoped_table_without_project_id_raises_a_clear_error():
    component = _build_component(project_id=None, tables="issues")
    with pytest.raises(ValueError, match="project-scoped"):
        component.build_defs(context=None)


def test_merge_requests_and_commits_also_require_project_id():
    component = _build_component(project_id=None, tables="merge_requests,commits")
    with pytest.raises(ValueError, match="project-scoped"):
        component.build_defs(context=None)


def test_projects_table_does_not_require_project_id():
    """'projects' is scoped by group_id (or left unscoped) -- not project_id."""
    component = _build_component(project_id=None, tables="projects")
    defs = component.build_defs(context=None)  # must not raise
    assert len(defs.assets) == 1


def test_table_name_map_with_an_unknown_table_raises_a_clear_error():
    component = _build_component(tables="issues", table_name_map={"commits": "raw_commits"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)


# --- request config: auth, base_url, paginator, endpoint paths --------------


def test_single_shape_builds_api_key_auth_and_header_link_paginator(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component()  # default asset_shape='single'
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    config = captured[-1]
    assert config["client"]["base_url"] == "https://gitlab.com/api/v4"
    assert config["client"]["auth"] == {
        "type": "api_key", "name": "PRIVATE-TOKEN", "api_key": "glpat-abc123", "location": "header",
    }
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
    assert isinstance(config["client"]["paginator"], HeaderLinkPaginator)
    assert config["resources"][0]["endpoint"]["path"] == "projects/my-group/proj/issues"
    assert config["resources"][0]["endpoint"]["params"]["per_page"] == 100

    df = result.output_for_node(asset_def.op.name)
    assert len(df) == 4
    assert set(df["_table"]) == {"issues"}


def test_self_managed_base_url_override_is_passed_through(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(base_url="https://gitlab.mycompany.com/api/v4/")  # trailing slash stripped
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["client"]["base_url"] == "https://gitlab.mycompany.com/api/v4"


def test_no_token_means_no_auth_in_client_config(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(access_token_env_var=None)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert "auth" not in captured[-1]["client"]


def test_commits_endpoint_path_and_filters(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    commits = pd.DataFrame({"id": ["sha1", "sha2"]})
    _patch_dlt(monkeypatch, commits, captured)

    component = _build_component(
        tables="commits", since="2024-01-01T00:00:00Z", until="2024-06-01T00:00:00Z", ref_name="main",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    endpoint = captured[-1]["resources"][0]["endpoint"]
    assert endpoint["path"] == "projects/my-group/proj/repository/commits"
    assert endpoint["params"]["since"] == "2024-01-01T00:00:00Z"
    assert endpoint["params"]["until"] == "2024-06-01T00:00:00Z"
    assert endpoint["params"]["ref_name"] == "main"


def test_issues_state_and_updated_after_filters(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(state="opened", updated_after="2024-01-01T00:00:00Z")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    endpoint = captured[-1]["resources"][0]["endpoint"]
    assert endpoint["params"]["state"] == "opened"
    assert endpoint["params"]["updated_after"] == "2024-01-01T00:00:00Z"


def test_projects_table_scoped_to_group_id(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    projects = pd.DataFrame({"id": [1, 2]})
    _patch_dlt(monkeypatch, projects, captured)

    component = _build_component(project_id=None, tables="projects", group_id="my-group")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["resources"][0]["endpoint"]["path"] == "groups/my-group/projects"


def test_projects_table_unscoped_lists_every_project(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    projects = pd.DataFrame({"id": [1, 2]})
    _patch_dlt(monkeypatch, projects, captured)

    component = _build_component(project_id=None, tables="projects")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["resources"][0]["endpoint"]["path"] == "projects"


# --- multi-table / asset_shape ----------------------------------------------


def test_asset_shape_defaults_to_single():
    component = _build_component(tables="issues,merge_requests")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_unions_every_table_tagged_by_table_column(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    issues = pd.DataFrame({"iid": [1, 2]})
    mrs = pd.DataFrame({"iid": [10]})
    _patch_dlt(monkeypatch, {"issues": issues, "merge_requests": mrs}, captured)

    component = _build_component(tables="issues,merge_requests", asset_shape="single")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["issues", "merge_requests"]  # always fetches every table together

    df = result.output_for_node(asset_def.op.name)
    assert dict(df.groupby("_table")["iid"].count()) == {"issues": 2, "merge_requests": 1}


def test_multiple_tables_each_become_their_own_independent_asset(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    issues = pd.DataFrame({"iid": [1, 2]})
    mrs = pd.DataFrame({"iid": [10]})
    _patch_dlt(monkeypatch, {"issues": issues, "merge_requests": mrs}, captured)

    component = _build_component(tables="issues,merge_requests", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_issues"), dg.AssetKey("test_asset_merge_requests")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["issues", "merge_requests"]

    issues_df = result.output_for_node(asset_def.op.name, output_name="test_asset_issues")
    mrs_df = result.output_for_node(asset_def.op.name, output_name="test_asset_merge_requests")
    assert list(issues_df["iid"]) == [1, 2]
    assert list(mrs_df["iid"]) == [10]


def test_materializing_one_table_never_fetches_the_other(monkeypatch):
    """The core requirement: selecting just one table must not even request the others from GitLab."""
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    issues = pd.DataFrame({"iid": [1, 2]})
    mrs = pd.DataFrame({"iid": [10]})
    _patch_dlt(monkeypatch, {"issues": issues, "merge_requests": mrs}, captured)

    component = _build_component(tables="issues,merge_requests", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_issues")],
    )
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["issues"]  # merge_requests was never requested at all

    issues_df = result.output_for_node(asset_def.op.name, output_name="test_asset_issues")
    assert list(issues_df["iid"]) == [1, 2]


def test_table_name_map_overrides_the_destination_table_name(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, {"raw_issues": FRAME}, captured)

    component = _build_component(table_name_map={"issues": "raw_issues"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resources = captured[-1]["resources"]
    assert resources[0]["name"] == "issues"
    assert resources[0]["table_name"] == "raw_issues"


# --- dlt-advanced-options: hash_columns / exclude_columns / on_schema_change / max_rows ----


class _Permissive:
    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter([self])


def test_on_schema_change_resolves_schema_contract_before_source_build(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(on_schema_change="block_all")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["resource_defaults"]["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_invalid_on_schema_change_raises():
    """on_schema_change is a Pydantic Literal field -- an invalid value is rejected at
    component-construction time, before build_defs/materialize ever run."""
    with pytest.raises(Exception):
        _build_component(on_schema_change="bogus")


class _ResourcesHolder:
    """Duck-typed stand-in for dlt's `source.resources` dict-of-DltResource -- only
    `.values()` is used by the hash/exclude/max-rows helpers."""

    def __init__(self, resources_list):
        self._list = resources_list

    def values(self):
        return self._list


def test_hash_columns_exclude_columns_and_max_rows_applied_to_resources(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline(FRAME, **kw))

    calls = {"add_map": 0, "add_limit": 0}

    class _TrackedResource:
        def add_map(self, *a, **k):
            calls["add_map"] += 1
            return self

        def add_limit(self, *a, **k):
            calls["add_limit"] += 1
            return self

    tracked = [_TrackedResource(), _TrackedResource()]

    class _FakeSource:
        resources = _ResourcesHolder(tracked)

    import dlt.sources.rest_api as _rest_api
    monkeypatch.setattr(_rest_api, "rest_api_source", lambda config: _FakeSource())

    component = _build_component(
        hash_columns=["title"],
        hash_salt="s3cr3t",
        exclude_columns=["updated_at"],
        max_rows_per_resource=50,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    # exclude_columns + hash_columns each add one map per resource; max_rows_per_resource adds one limit per resource.
    assert calls["add_map"] == 2 * len(tracked)
    assert calls["add_limit"] == len(tracked)


def test_hash_columns_without_salt_raises_a_clear_error(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")

    class _FakeSource:
        resources = _ResourcesHolder([_Permissive()])

    import dlt.sources.rest_api as _rest_api
    monkeypatch.setattr(_rest_api, "rest_api_source", lambda config: _FakeSource())
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline(FRAME, **kw))

    component = _build_component(hash_columns=["title"])  # no hash_salt / hash_salt_env_var
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(ValueError, match="requires a salt|hash_salt"):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


# --- partitioning: cosmetic-partitioning regression --------------------------


def test_partition_date_column_actually_filters_the_dataframe(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    captured = []
    _patch_dlt(monkeypatch, FRAME, captured)

    component = _build_component(
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="updated_at",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def],
        partition_key="2024-01-01",
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    df = result.output_for_node(asset_def.op.name)
    assert set(df["iid"]) == {1, 2}  # only the 2024-01-01 rows, not all 4
