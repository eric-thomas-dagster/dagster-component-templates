"""Real, committed tests for DagsterDiscoveryToolsComponent.

Mocks ONLY the GraphQL transport (a fake client's `.call`/`.introspect_type`, keyed by which top-level
field the query asks for) -- every other behavior (response parsing, client-side filtering, the
introspection-driven auto_selection() builder for Dagster+-only fields, the allow-list/error-shaping
dispatcher) runs for real.
"""
import pathlib
import sys

import pytest

from .conftest import load_component_module

mod = load_component_module()

DagsterDiscoveryTools = mod.DagsterDiscoveryTools
DiscoveryToolError = mod.DiscoveryToolError
auto_selection = mod.auto_selection
resolve_deployment_url = mod.resolve_deployment_url
validate_deployment_url = mod.validate_deployment_url


class _FakeClient:
    """Routes a GraphQL `call()` to a canned response by which top-level field the query selects, and
    `introspect_type()` to a canned __type shape by name -- lets tests drive real parsing/assembly logic
    in DagsterDiscoveryTools without a real GraphQL server or schema."""

    def __init__(self, call_responses=None, type_responses=None):
        self.call_responses = call_responses or {}
        self.type_responses = type_responses or {}
        self.calls = []

    def call(self, query, variables=None):
        self.calls.append((query, variables))
        for field, response in self.call_responses.items():
            if field in query:
                return response(variables) if callable(response) else response
        raise AssertionError(f"no canned response for query: {query[:200]}")

    def introspect_type(self, type_name):
        return self.type_responses.get(type_name)


# ── deployment URL resolution (sanity -- real logic, not mocked) ────────────────────────────────────────────────

def test_validate_deployment_url_rejects_non_https_non_localhost():
    with pytest.raises(ValueError, match="must be a full https URL"):
        validate_deployment_url("http://acme.dagster.cloud/prod")


def test_validate_deployment_url_allows_http_localhost():
    assert validate_deployment_url("http://localhost:3000/prod") == "http://localhost:3000/prod"


def test_resolve_deployment_url_auto_requires_deployment_env(monkeypatch):
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    with pytest.raises(ValueError, match="DAGSTER_CLOUD_DEPLOYMENT_NAME is not set"):
        resolve_deployment_url("auto", instance=None, env={})


def test_resolve_deployment_url_auto_derives_from_instance_dagit_url():
    class _FakeInstance:
        dagit_url = "https://acme.dagster.cloud"

    url = resolve_deployment_url("auto", instance=_FakeInstance(), env={"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"})
    assert url == "https://acme.dagster.cloud/prod"


def test_resolve_deployment_url_explicit_value_wins_over_auto_env():
    assert resolve_deployment_url("https://acme.dagster.cloud/staging", env={"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"}) == "https://acme.dagster.cloud/staging"


# ── list_locations (real OSS workspaceOrError shape) ────────────────────────────────────────────────────────────

def test_list_locations_parses_workspace_shape_including_load_errors():
    client = _FakeClient(call_responses={
        "workspaceOrError": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {
                        "name": "ingestion",
                        "loadStatus": "LOADED",
                        "locationOrLoadError": {
                            "__typename": "RepositoryLocation",
                            "dagsterLibraryVersions": [{"name": "dagster", "version": "1.13.0"}],
                            "repositories": [{"name": "__repository__", "schedules": [{"name": "s1"}], "sensors": []}],
                        },
                    },
                    {
                        "name": "broken_location",
                        "loadStatus": "FAILED",
                        "locationOrLoadError": {"__typename": "PythonError", "message": "ImportError: no module named foo"},
                    },
                ],
            }
        }
    })
    tools = DagsterDiscoveryTools(client, allow=["list_locations"])
    result = tools.call("list_locations")
    assert result["count"] == 2
    ok = next(l for l in result["locations"] if l["name"] == "ingestion")
    assert ok["library_versions"] == {"dagster": "1.13.0"}
    assert ok["repositories"] == [{"name": "__repository__", "schedule_count": 1, "sensor_count": 0}]
    broken = next(l for l in result["locations"] if l["name"] == "broken_location")
    assert broken["load_error"] == "ImportError: no module named foo"


# ── list_schedules_and_sensors ───────────────────────────────────────────────────────────────────────────────────

def test_list_schedules_and_sensors_filters_by_location():
    client = _FakeClient(call_responses={
        "workspaceOrError": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {"name": "loc_a", "locationOrLoadError": {"__typename": "RepositoryLocation", "repositories": [
                        {"name": "r1", "schedules": [{"name": "daily", "cronSchedule": "0 0 * * *", "description": None}], "sensors": []},
                    ]}},
                    {"name": "loc_b", "locationOrLoadError": {"__typename": "RepositoryLocation", "repositories": [
                        {"name": "r2", "schedules": [], "sensors": [{"name": "watcher", "description": None, "sensorType": "SENSOR"}]},
                    ]}},
                ],
            }
        }
    })
    tools = DagsterDiscoveryTools(client, allow=["list_schedules_and_sensors"])
    result = tools.call("list_schedules_and_sensors", {"location_name": "loc_b"})
    assert result["count"] == 1
    assert result["repositories"][0]["sensors"][0]["name"] == "watcher"


# ── exclude_locations (so an agent never lists, reviews, or reacts to its own code location) ────────────────────

def test_exclude_locations_drops_the_named_location_from_list_locations():
    client = _FakeClient(call_responses={
        "workspaceOrError": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {"name": "agents", "loadStatus": "LOADED", "locationOrLoadError": {"__typename": "RepositoryLocation", "dagsterLibraryVersions": [], "repositories": []}},
                    {"name": "ingestion", "loadStatus": "LOADED", "locationOrLoadError": {"__typename": "RepositoryLocation", "dagsterLibraryVersions": [], "repositories": []}},
                ],
            }
        }
    })
    tools = DagsterDiscoveryTools(client, allow=["list_locations"], exclude_locations=["agents"])
    result = tools.call("list_locations")
    assert result["count"] == 1
    assert [l["name"] for l in result["locations"]] == ["ingestion"]


def test_exclude_locations_drops_the_named_location_from_list_schedules_and_sensors():
    client = _FakeClient(call_responses={
        "workspaceOrError": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {"name": "agents", "locationOrLoadError": {"__typename": "RepositoryLocation", "repositories": [
                        {"name": "r1", "schedules": [{"name": "weekly_review", "cronSchedule": "0 9 * * 1", "description": None}], "sensors": []},
                    ]}},
                    {"name": "ingestion", "locationOrLoadError": {"__typename": "RepositoryLocation", "repositories": [
                        {"name": "r2", "schedules": [], "sensors": [{"name": "watcher", "description": None, "sensorType": "SENSOR"}]},
                    ]}},
                ],
            }
        }
    })
    tools = DagsterDiscoveryTools(client, allow=["list_schedules_and_sensors"], exclude_locations=["agents"])
    result = tools.call("list_schedules_and_sensors")
    assert result["count"] == 1
    assert result["repositories"][0]["location_name"] == "ingestion"


def test_exclude_locations_refuses_list_runs_for_the_excluded_location():
    tools = DagsterDiscoveryTools(_FakeClient(), allow=["list_runs"], exclude_locations=["agents"])
    result = tools.call("list_runs", {"location_name": "agents"})
    assert "error" in result
    assert "excluded" in result["error"]


# ── list_runs ─────────────────────────────────────────────────────────────────────────────────────────────────

def test_list_runs_requires_location_name():
    tools = DagsterDiscoveryTools(_FakeClient(), allow=["list_runs"])
    result = tools.call("list_runs", {})
    assert "error" in result
    assert "location_name" in result["error"]


def test_list_runs_builds_repository_origin_filter_and_parses_results():
    captured = {}

    def _runs(variables):
        captured.update(variables)
        return {"runsOrError": {"__typename": "Runs", "results": [
            {"runId": "abc123", "jobName": "my_job", "status": "FAILURE", "startTime": 1.0, "endTime": 2.0},
        ]}}

    client = _FakeClient(call_responses={"runsOrError": _runs})
    tools = DagsterDiscoveryTools(client, allow=["list_runs"])
    result = tools.call("list_runs", {"location_name": "ingestion", "limit": 5, "statuses": ["failure"]})
    assert captured["filter"]["repositoryOrigin"] == {"repositoryLocationName": "ingestion"}
    assert captured["filter"]["statuses"] == ["FAILURE"]
    assert captured["limit"] == 5
    assert result["count"] == 1
    assert result["runs"][0]["runId"] == "abc123"


def test_list_runs_python_error_raises_tool_error():
    client = _FakeClient(call_responses={"runsOrError": {"runsOrError": {"__typename": "PythonError", "message": "boom"}}})
    tools = DagsterDiscoveryTools(client, allow=["list_runs"])
    result = tools.call("list_runs", {"location_name": "x"})
    assert result["error"] == "runsOrError: boom"


# ── auto_selection: introspection-driven, no hard-coded Cloud-only type shapes ──────────────────────────────────

def test_auto_selection_builds_scalar_fields_from_object_type():
    client = _FakeClient(type_responses={
        "Widget": {
            "name": "Widget", "kind": "OBJECT",
            "fields": [
                {"name": "id", "args": [], "type": {"kind": "NON_NULL", "name": None, "ofType": {"kind": "SCALAR", "name": "ID", "ofType": None}}},
                {"name": "label", "args": [], "type": {"kind": "SCALAR", "name": "String", "ofType": None}},
            ],
        }
    })
    selection = auto_selection(client, "Widget", depth=2)
    assert "id" in selection
    assert "label" in selection


def test_auto_selection_skips_fields_with_required_arguments():
    client = _FakeClient(type_responses={
        "Widget": {
            "name": "Widget", "kind": "OBJECT",
            "fields": [
                {"name": "label", "args": [], "type": {"kind": "SCALAR", "name": "String", "ofType": None}},
                {"name": "byId", "args": [{"name": "id", "type": {"kind": "NON_NULL", "ofType": {"kind": "SCALAR"}}}],
                 "type": {"kind": "SCALAR", "name": "String", "ofType": None}},
            ],
        }
    })
    selection = auto_selection(client, "Widget", depth=2)
    assert "label" in selection
    assert "byId" not in selection


def test_auto_selection_builds_union_fragments_from_possible_types_discovered_live():
    client = _FakeClient(type_responses={
        "WidgetOrError": {
            "name": "WidgetOrError", "kind": "UNION",
            "possibleTypes": [{"name": "WidgetSuccess"}, {"name": "SomeError"}],
        },
        "WidgetSuccess": {
            "name": "WidgetSuccess", "kind": "OBJECT",
            "fields": [{"name": "widgets", "args": [], "type": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Widget"}}}],
        },
        "SomeError": {
            "name": "SomeError", "kind": "OBJECT",
            "fields": [{"name": "message", "args": [], "type": {"kind": "SCALAR", "name": "String", "ofType": None}}],
        },
        "Widget": {
            "name": "Widget", "kind": "OBJECT",
            "fields": [{"name": "label", "args": [], "type": {"kind": "SCALAR", "name": "String", "ofType": None}}],
        },
    })
    selection = auto_selection(client, "WidgetOrError", depth=3)
    assert "__typename" in selection
    assert "... on WidgetSuccess" in selection
    assert "... on SomeError { message }" in selection
    assert "label" in selection  # recursed into Widget via WidgetSuccess.widgets


def test_auto_selection_bottoms_out_at_depth_zero_without_crashing():
    client = _FakeClient()
    assert auto_selection(client, "Anything", depth=0) == "__typename"


# ── list_components / get_component / list_agents / list_mcp_servers ────────────────────────────────────────────

def _components_client(rows):
    return _FakeClient(
        type_responses={
            "AppManagedComponentsForLocationOrError": {
                "name": "AppManagedComponentsForLocationOrError", "kind": "UNION",
                "possibleTypes": [{"name": "AppManagedComponentsForLocationSuccess"}, {"name": "PythonError"}],
            },
        },
        call_responses={
            "appManagedComponentsForLocationOrError": {
                "appManagedComponentsForLocationOrError": {
                    "__typename": "AppManagedComponentsForLocationSuccess",
                    "components": rows,
                }
            },
        },
    )


_ROWS = [
    {"id": "1", "type": "dagster_community_components.OpenAIAgentComponent", "yaml": "model: gpt-4"},
    {"id": "2", "type": "dagster_community_components.SlackChatToolsComponent", "yaml": "x: y"},
    {"id": "3", "type": "dagster_community_components.S3ToDatabaseAssetComponent", "yaml": "z: w"},
]


def test_list_components_returns_everything_by_default():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["list_components"])
    result = tools.call("list_components", {"location_name": "ingestion"})
    assert result["count"] == 3


def test_list_components_filters_by_type_substring():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["list_components"])
    result = tools.call("list_components", {"location_name": "ingestion", "type": "agent"})
    assert result["count"] == 1
    assert result["components"][0]["id"] == "1"


def test_get_component_returns_the_matching_row():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["get_component"])
    result = tools.call("get_component", {"location_name": "ingestion", "id": "2"})
    assert result["type"].endswith("SlackChatToolsComponent")


def test_get_component_missing_id_raises_clear_error():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["get_component"])
    result = tools.call("get_component", {"location_name": "ingestion", "id": "999"})
    assert "no component with id" in result["error"]


def test_list_agents_classifies_by_type_name():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["list_agents"])
    result = tools.call("list_agents", {"location_name": "ingestion"})
    assert result["count"] == 1
    assert "Agent" in result["agents"][0]["type"]


def test_list_mcp_servers_classifies_by_type_name():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["list_mcp_servers"])
    result = tools.call("list_mcp_servers", {"location_name": "ingestion"})
    assert result["count"] == 1
    assert "ChatTools" in result["mcp_servers"][0]["type"]


def test_list_components_location_name_required():
    tools = DagsterDiscoveryTools(_components_client(_ROWS), allow=["list_components"])
    result = tools.call("list_components", {})
    assert "error" in result
    assert "location_name is required" in result["error"]


# ── dispatcher: allow-list + unknown tool + error shaping ───────────────────────────────────────────────────────

def test_unknown_tool_returns_error_not_an_exception():
    tools = DagsterDiscoveryTools(_FakeClient())
    result = tools.call("delete_everything")
    assert "unknown tool" in result["error"]


def test_tool_not_in_allow_list_is_refused():
    tools = DagsterDiscoveryTools(_FakeClient(), allow=["list_locations"])
    result = tools.call("list_runs", {"location_name": "x"})
    assert "not enabled" in result["error"]


def test_constructing_with_unknown_allow_entry_raises():
    with pytest.raises(ValueError, match="unknown tool"):
        DagsterDiscoveryTools(_FakeClient(), allow=["not_a_real_tool"])


def test_result_over_max_chars_is_truncated_to_a_preview():
    client = _FakeClient(call_responses={
        "workspaceOrError": {"workspaceOrError": {"__typename": "Workspace", "locationEntries": [
            {"name": f"loc_{i}", "loadStatus": "LOADED", "locationOrLoadError": {"__typename": "RepositoryLocation", "dagsterLibraryVersions": [], "repositories": []}}
            for i in range(200)
        ]}}
    })
    tools = DagsterDiscoveryTools(client, allow=["list_locations"], max_result_chars=500)
    result = tools.call("list_locations")
    assert result["truncated"] is True
    assert "preview" in result
