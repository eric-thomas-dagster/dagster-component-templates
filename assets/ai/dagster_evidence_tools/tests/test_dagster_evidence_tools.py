"""Real, committed tests for DagsterEvidenceToolsComponent.

Mocks ONLY the HTTP boundary -- a fake GraphQL transport (`.call`, keyed by which real query the component
actually sends, same style as dagster_discovery_tools' own tests) plus `requests.request`/`requests.get`/
`requests.post` (the three real calls this component makes: GitHub's REST API, PyPI's JSON API, OSV.dev).
Every other behavior -- asset/schedule/check/partition shaping, retry/incident/runtime-change windowing,
the deprecated-API ast scan, the allow-list/error-shaping dispatcher -- runs for real.
"""
import base64
import json
import re

import pytest
import requests

from .conftest import load_component_module

mod = load_component_module()

DAY = 86400.0
FIXED_NOW = 2_000_000.0


# ── fakes ────────────────────────────────────────────────────────────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body) if not isinstance(body, str) else body
        self.content = b"1" if body is not None else b""
        self.ok = status_code < 400

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeGithubApi:
    """A tiny in-memory GitHub REST API: only the two endpoints this component calls --
    `GET /git/trees/{ref}?recursive=1` and `GET /contents/{path}?ref=...`."""

    def __init__(self, repo, tree, contents):
        self.repo = repo
        self.tree = tree  # list of {"path", "type"}
        self.contents = contents  # path -> text content
        self.calls = []

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        self.calls.append((method, url))
        path = url.split(f"/repos/{self.repo}", 1)[-1]
        if method == "GET" and re.match(r"^/git/trees/", path):
            return _FakeResp(200, {"tree": self.tree})
        m = re.match(r"^/contents/(.*)$", path)
        if method == "GET" and m:
            p = m.group(1)
            if p not in self.contents:
                return _FakeResp(404, {"message": "Not Found"})
            return _FakeResp(200, {"sha": "filesha", "content": base64.b64encode(self.contents[p].encode()).decode()})
        raise AssertionError(f"FakeGithubApi: no handler for {method} {path}")


class _FakeGraphQLClient:
    """Routes a GraphQL `call()` to a canned response by which REAL query component.py sends (every query
    this component builds has a distinguishing, non-overlapping substring)."""

    def __init__(self, assets_response=None, workspace_response=None, runs_response=None,
                 retry_events_by_run=None, failure_events_by_run=None, alert_policies=None):
        self.alert_policies = [] if alert_policies is None else alert_policies  # a list, or an Exception to raise
        self.assets_response = assets_response or {}
        self.workspace_response = workspace_response or {}
        self.runs_response = runs_response or {}
        self.retry_events_by_run = retry_events_by_run or {}
        self.failure_events_by_run = failure_events_by_run or {}
        self.calls = []

    def call(self, query, variables=None):
        self.calls.append((query, variables))
        run_id = (variables or {}).get("id")
        if "ExecutionStepUpForRetryEvent" in query:
            return {"logsForRun": self.retry_events_by_run.get(run_id, {"__typename": "EventConnection", "events": []})}
        if "ExecutionStepFailureEvent" in query:
            return {"logsForRun": self.failure_events_by_run.get(run_id, {"__typename": "EventConnection", "events": []})}
        if "alertPoliciesForCodeLocation" in query:
            if isinstance(self.alert_policies, Exception):
                raise self.alert_policies
            return {"alertPoliciesForCodeLocation": self.alert_policies}
        if "runsOrError" in query:
            return self.runs_response
        if "workspaceOrError" in query:
            return self.workspace_response
        if "assetNodes" in query:
            return self.assets_response
        raise AssertionError(f"no canned response for query: {query[:200]}")


def _deny_http(*_a, **_k):
    raise AssertionError("no HTTP call should be made here")


# ── fixtures: the "ingestion" location's canned deployment -────────────────────────────────────────────────────────

ASSETS_RESPONSE = {
    "assetNodes": [
        {
            "assetKey": {"path": ["raw", "orders"]},
            "groupName": "ingestion",
            "jobNames": ["ingest_job"],
            "freshnessPolicy": {"maximumLagMinutes": 60.0, "cronSchedule": None},
            "automationCondition": {"label": "eager"},
            "hasAssetChecks": True,
            "partitionStats": {"numMaterialized": 8, "numPartitions": 10, "numFailed": 1, "numMaterializing": 0},
            "metadataEntries": [
                {"__typename": "CodeReferencesMetadataEntry", "codeReferences": [
                    {"__typename": "LocalFileCodeReference", "filePath": "assets/orders.py", "lineNumber": 12},
                ]},
                {"__typename": "TextMetadataEntry"},
            ],
            "assetChecksOrError": {
                "__typename": "AssetChecks",
                "checks": [
                    {"name": "row_count_check", "executionForLatestMaterialization": {"status": "SUCCEEDED", "timestamp": 1.0}},
                    {"name": "freshness_check", "executionForLatestMaterialization": {"status": "FAILED", "timestamp": 2.0}},
                ],
            },
            "repository": {"location": {"name": "ingestion"}},
        },
        {
            # a different code location -- must never leak into "ingestion"'s overview
            "assetKey": {"path": ["other", "thing"]},
            "groupName": "other",
            "jobNames": [],
            "freshnessPolicy": None,
            "automationCondition": None,
            "hasAssetChecks": False,
            "partitionStats": None,
            "metadataEntries": [],
            "assetChecksOrError": {"__typename": "AssetChecks", "checks": []},
            "repository": {"location": {"name": "other_location"}},
        },
    ]
}

WORKSPACE_RESPONSE = {
    "workspaceOrError": {
        "__typename": "Workspace",
        "locationEntries": [
            {
                "name": "ingestion",
                "locationOrLoadError": {
                    "__typename": "RepositoryLocation",
                    "dagsterLibraryVersions": [{"name": "dagster", "version": "1.8.0"}, {"name": "dagster-aws", "version": "0.24.0"}],
                    "repositories": [{"schedules": [
                        {"name": "daily_ingest", "cronSchedule": "0 0 * * *", "pipelineName": "ingest_job", "scheduleState": {"status": "RUNNING"}},
                    ]}],
                },
            },
        ],
    }
}

RUNS_RESPONSE = {
    "runsOrError": {
        "__typename": "Runs",
        "results": [
            {"runId": "run_retry_ok", "status": "SUCCESS", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - 1 * DAY, "endTime": FIXED_NOW - 1 * DAY + 120,
             "assetSelection": [{"path": ["raw", "retried_asset"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_failed", "status": "FAILURE", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - 3600, "endTime": FIXED_NOW - 3500,
             "assetSelection": [{"path": ["raw", "failed_asset"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_old_1", "status": "SUCCESS", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - 10 * DAY, "endTime": FIXED_NOW - 10 * DAY + 50,
             "assetSelection": [{"path": ["raw", "orders"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_old_2", "status": "SUCCESS", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - 9 * DAY, "endTime": FIXED_NOW - 9 * DAY + 60,
             "assetSelection": [{"path": ["raw", "orders"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_new_1", "status": "SUCCESS", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - 2 * DAY, "endTime": FIXED_NOW - 2 * DAY + 200,
             "assetSelection": [{"path": ["raw", "orders"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_new_2", "status": "SUCCESS", "pipelineName": "ingest_job",
             "startTime": FIXED_NOW - int(1.5 * DAY), "endTime": FIXED_NOW - int(1.5 * DAY) + 220,
             "assetSelection": [{"path": ["raw", "orders"]}],
             "repositoryOrigin": {"repositoryLocationName": "ingestion"}},
            {"runId": "run_other_loc", "status": "FAILURE", "pipelineName": "x",
             "startTime": FIXED_NOW - 100, "endTime": FIXED_NOW - 50,
             "assetSelection": [], "repositoryOrigin": {"repositoryLocationName": "other_location"}},
        ],
    }
}

RETRY_EVENTS = {
    "run_retry_ok": {"__typename": "EventConnection", "events": [
        {"__typename": "ExecutionStepUpForRetryEvent", "stepKey": "load_orders",
         "error": {"message": "top msg", "cause": None, "causes": [{"message": "root cause: connection reset"}]}},
    ]},
}

FAILURE_EVENTS = {
    "run_failed": {"__typename": "EventConnection", "events": [
        {"__typename": "ExecutionStepFailureEvent", "stepKey": "extract_orders",
         "error": {"message": "boom top", "cause": None, "causes": []}},
    ]},
}

PYPROJECT_TOML = """
[project]
name = "hooli-data-eng-pipelines"
dependencies = [
    "dagster==1.8.0",
    "requests>=2.25,<3",
    "pandas",
]
"""


def _overview_tools(github_api, **overrides):
    client = _FakeGraphQLClient(ASSETS_RESPONSE, WORKSPACE_RESPONSE, RUNS_RESPONSE, RETRY_EVENTS, FAILURE_EVENTS)
    kwargs = dict(
        deployment_url="https://acme.dagster.cloud/prod", repo=github_api.repo if github_api else None,
        github_token="ghtoken", repo_notes="Demo repo; ignore the deliberate flakiness.",
        now=lambda: FIXED_NOW,
    )
    kwargs.update(overrides)
    return mod.DagsterEvidenceTools(client, **kwargs), client


def _pypi_github_release(monkeypatch, version="1.9.5", notes="release notes here"):
    def fake_get(url, headers=None, timeout=None, **kwargs):
        if url == "https://pypi.org/pypi/dagster/json":
            return _FakeResp(200, {"info": {"version": version}})
        if url == f"https://api.github.com/repos/dagster-io/dagster/releases/tags/{version}":
            return _FakeResp(200, {"body": notes, "html_url": f"https://github.com/dagster-io/dagster/releases/tag/{version}"})
        raise AssertionError(f"unexpected GET {url}")
    monkeypatch.setattr("requests.get", fake_get)


def _osv(monkeypatch, fixed_in="1.9.0"):
    def fake_post(url, json=None, timeout=None, **kwargs):  # noqa: A002 -- matches requests.post's own kwarg name
        assert url == "https://api.osv.dev/v1/query"
        assert json["package"]["ecosystem"] == "PyPI"
        if json["package"]["name"] == "dagster":
            return _FakeResp(200, {"vulns": [{"id": "GHSA-xxxx-yyyy-zzzz", "affected": [
                {"ranges": [{"events": [{"introduced": "0"}, {"fixed": fixed_in}]}]}
            ]}]})
        return _FakeResp(200, {"vulns": []})
    monkeypatch.setattr("requests.post", fake_post)


# ── project_overview: full happy path, every section ────────────────────────────────────────────────────────────

def test_project_overview_returns_every_section_correctly_shaped(monkeypatch):
    github_api = FakeGithubApi(
        repo="acme/widgets",
        tree=[{"path": "pyproject.toml", "type": "blob"}, {"path": "uv.lock", "type": "blob"}, {"path": "src", "type": "tree"}],
        contents={"pyproject.toml": PYPROJECT_TOML},
    )
    monkeypatch.setattr("requests.request", github_api.request)
    _pypi_github_release(monkeypatch)
    _osv(monkeypatch)

    tools, client = _overview_tools(github_api)
    result = tools.call("project_overview", {"location": "ingestion"})
    assert "error" not in result, result

    assert result["code_location"] == "ingestion"
    assert result["repo_notes"] == "Demo repo; ignore the deliberate flakiness."

    # assets: freshness + automation + schedules + code references -- only this location's asset
    assert len(result["assets"]) == 1
    asset = result["assets"][0]
    assert asset["asset_key"] == "raw/orders"
    assert asset["freshness_policy"] == {"maximumLagMinutes": 60.0, "cronSchedule": None}
    assert asset["automation_condition"] == "eager"
    assert asset["scheduled_by"] == ["daily_ingest"]
    assert asset["code_references"] == [{"path": "assets/orders.py", "line": 12}]
    assert asset["asset_checks"] == {"passing": 1, "failing": 1, "stale": 0, "failing_checks": [{"name": "freshness_check", "status": "FAILED"}]}
    assert asset["partition_health"] == {"partitions": 10, "materialized": 8, "failed": 1, "missing": 1}

    assert result["schedules"] == [{"name": "daily_ingest", "cron": "0 0 * * *", "job": "ingest_job", "status": "RUNNING"}]
    assert result["running_dagster_version"] == "1.8.0"
    assert result["latest_dagster_version"] == "1.9.5"
    assert result["dagster_dependency"] == "dagster==1.8.0"

    # retries: who retried, why, and whether the run ultimately succeeded
    retries = result["retries"]
    assert retries["window_days"] == 7
    assert len(retries["steps"]) == 1
    step = retries["steps"][0]
    assert step["step"] == "load_orders"
    assert step["runs_with_retries"] == 1
    assert step["retry_events"] == 1
    assert step["final_run_status"] == {"SUCCESS": 1}
    assert step["errors"][0]["message"] == "root cause: connection reset"

    # partition + asset-check health: pass/fail/stale counts, stale/failing surfaced
    health = result["partition_and_check_health"]
    assert health["checks_passing"] == 1
    assert health["checks_failing"] == 1
    assert health["checks_stale"] == 0
    assert health["partitions_needing_attention"] == [{"asset_key": "raw/orders", "partitions": 10, "materialized": 8, "failed": 1, "missing": 1}]
    assert health["assets_with_failing_checks"] == [{"asset_key": "raw/orders", "failing_checks": [{"name": "freshness_check", "status": "FAILED"}]}]

    # runtime changes: the later half of "raw/orders" runs got slower
    runtime = result["runtime_changes"]
    assert runtime["slower"] == [{"runs_of": "raw/orders", "median_seconds_before": 55, "median_seconds_after": 210}]

    # package risks: declared deps, running version (dagster-family only), OSV vuln + fixed-in version
    deps = result["dependencies"]
    by_name = {d["name"]: d for d in deps["declared_dependencies"]}
    assert by_name["dagster"] == {"name": "dagster", "spec": "dagster==1.8.0", "kind": "pinned", "running_version": "1.8.0"}
    assert by_name["requests"]["running_version"] is None  # not in dagsterLibraryVersions -- no fabricated version
    assert by_name["pandas"]["kind"] == "unconstrained"
    assert deps["lock_files"] == ["uv.lock"]
    assert deps["has_lock_file"] is True
    assert deps["running_packages_checked_against_osv"] == 1  # only dagster has a known running version
    assert deps["running_packages_with_known_vulnerabilities"] == [
        {"package": "dagster", "running_version": "1.8.0", "advisories": ["GHSA-xxxx-yyyy-zzzz"], "fixed_in": ["1.9.0"]}
    ]

    # incidents: failed runs in the last 24h, with the failing step + error
    incidents = result["incidents"]
    assert incidents["window_hours"] == 24
    assert incidents["failed_runs"] == 1
    assert incidents["incidents"] == [{
        "run": "https://acme.dagster.cloud/prod/runs/run_failed", "job": "ingest_job",
        "assets": ["raw/failed_asset"], "failed_step": "extract_orders", "error": "boom top",
    }]


def test_project_overview_excluded_location_refuses_with_zero_http_calls(monkeypatch):
    monkeypatch.setattr("requests.request", _deny_http)
    monkeypatch.setattr("requests.get", _deny_http)
    monkeypatch.setattr("requests.post", _deny_http)
    client = _FakeGraphQLClient()
    tools = mod.DagsterEvidenceTools(client, exclude_locations=["evidence_advisor"])
    result = tools.call("project_overview", {"location": "evidence_advisor"})
    assert "error" in result
    assert "excluded" in result["error"]
    assert client.calls == []


def test_project_overview_requires_location():
    client = _FakeGraphQLClient()
    tools = mod.DagsterEvidenceTools(client)
    result = tools.call("project_overview", {})
    assert "error" in result
    assert "location" in result["error"]


def test_project_overview_dependencies_skipped_without_repo(monkeypatch):
    monkeypatch.setattr("requests.request", _deny_http)
    _pypi_github_release(monkeypatch)
    tools, _ = _overview_tools(github_api=None, repo=None)
    result = tools.call("project_overview", {"location": "ingestion"})
    assert "error" not in result, result
    assert result["dependencies"] == {"configured": False, "note": "`repo` not configured -- skipping dependency/package-risk scan."}
    assert result["dagster_dependency"] is None


# ── deprecation_report ──────────────────────────────────────────────────────────────────────────────────────────

def test_deprecation_report_requires_repo():
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient())
    result = tools.call("deprecation_report")
    assert "error" in result
    assert "repo" in result["error"]


def test_deprecation_report_finds_a_removed_dagster_import(monkeypatch):
    github_api = FakeGithubApi(
        repo="acme/widgets",
        tree=[{"path": "src/ops.py", "type": "blob"}, {"path": "src", "type": "tree"}],
        contents={"src/ops.py": "from dagster import ThisDoesNotExistAnymoreXYZ\n"},
    )
    monkeypatch.setattr("requests.request", github_api.request)
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient(), repo="acme/widgets", github_token="ghtoken")
    result = tools.call("deprecation_report")
    assert "error" not in result, result
    assert result["files_scanned"] == 1
    assert len(result["findings"]) == 1
    finding = result["findings"][0]
    assert finding["path"] == "src/ops.py"
    assert finding["name"] == "dagster.ThisDoesNotExistAnymoreXYZ"
    assert finding["status"] == "removed"


def test_deprecation_report_respects_exclude_paths(monkeypatch):
    github_api = FakeGithubApi(
        repo="acme/widgets",
        tree=[{"path": "vendor/old.py", "type": "blob"}, {"path": "src/ops.py", "type": "blob"}],
        contents={"vendor/old.py": "from dagster import NopeNotReal\n", "src/ops.py": "x = 1\n"},
    )
    monkeypatch.setattr("requests.request", github_api.request)
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient(), repo="acme/widgets", github_token="ghtoken", exclude_paths=["vendor/"])
    result = tools.call("deprecation_report")
    assert result["files_scanned"] == 1
    assert result["findings"] == []


# ── latest_dagster_release ──────────────────────────────────────────────────────────────────────────────────────

def test_latest_dagster_release_real_shape(monkeypatch):
    _pypi_github_release(monkeypatch, version="1.9.5", notes="## Changelog\n- big new feature")
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient())
    result = tools.call("latest_dagster_release")
    assert result == {
        "latest_version": "1.9.5",
        "notes": "## Changelog\n- big new feature",
        "url": "https://github.com/dagster-io/dagster/releases/tag/1.9.5",
    }


def test_latest_dagster_release_tolerates_github_release_lookup_failing(monkeypatch):
    def fake_get(url, headers=None, timeout=None, **kwargs):
        if url == "https://pypi.org/pypi/dagster/json":
            return _FakeResp(200, {"info": {"version": "1.9.5"}})
        return _FakeResp(404, {"message": "Not Found"})
    monkeypatch.setattr("requests.get", fake_get)
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient())
    result = tools.call("latest_dagster_release")
    assert result["latest_version"] == "1.9.5"
    assert result["notes"] == ""


# ── dispatcher: allow-list + unknown tool + size cap (same conventions as dagster_discovery_tools) ──────────────

def test_unknown_tool_returns_error_not_an_exception():
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient())
    result = tools.call("delete_everything")
    assert "unknown tool" in result["error"]


def test_tool_not_in_allow_list_is_refused():
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient(), allow=["latest_dagster_release"])
    result = tools.call("project_overview", {"location": "x"})
    assert "not enabled" in result["error"]


def test_constructing_with_unknown_allow_entry_raises():
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DagsterEvidenceTools(_FakeGraphQLClient(), allow=["not_a_real_tool"])


def test_result_over_max_chars_is_truncated_to_a_preview(monkeypatch):
    _pypi_github_release(monkeypatch, notes="x" * 10000)
    tools = mod.DagsterEvidenceTools(_FakeGraphQLClient(), allow=["latest_dagster_release"], max_result_chars=500)
    result = tools.call("latest_dagster_release")
    assert result["truncated"] is True
    assert "preview" in result


# ── repo_notes passthrough ───────────────────────────────────────────────────────────────────────────────────────

def test_repo_notes_passes_through_component_config_unchanged(monkeypatch):
    monkeypatch.setattr("requests.request", _deny_http)
    _pypi_github_release(monkeypatch)
    tools, _ = _overview_tools(github_api=None, repo=None, repo_notes="Flakiness in retry_handler.py is deliberate.")
    result = tools.call("project_overview", {"location": "ingestion"})
    assert result["repo_notes"] == "Flakiness in retry_handler.py is deliberate."


# ── Dagster+ schema validation + alert policies ────────────────────────────────────────────────────────────────────

_SCHEMA_SUBSET = __import__("pathlib").Path(__file__).parent / "fixtures" / "dagster_plus_schema_subset.graphql"


def _component_queries():
    return {n: v for n, v in vars(mod).items() if n.startswith("_") and n.endswith("_QUERY") and isinstance(v, str)}


def test_every_graphql_query_validates_against_the_dagster_plus_schema():
    # Dagster+ is stricter than OSS dagster_graphql: it rejected `... on PythonError` under assetChecksOrError with an
    # HTTP 400 on every project_overview call. Validate every query this component sends against the real Dagster+ schema.
    graphql = pytest.importorskip("graphql")
    schema = graphql.build_schema(_SCHEMA_SUBSET.read_text())
    queries = _component_queries()
    assert {"_ASSETS_QUERY", "_WORKSPACE_QUERY", "_RUNS_QUERY", "_ALERT_POLICIES_QUERY"} <= set(queries)
    for name, query in queries.items():
        errors = graphql.validate(schema, graphql.parse(query))
        assert not errors, f"{name} is not valid against the Dagster+ schema: {[e.message for e in errors]}"


def test_the_schema_check_would_have_caught_the_pythonerror_fragment():
    graphql = pytest.importorskip("graphql")
    schema = graphql.build_schema(_SCHEMA_SUBSET.read_text())
    bad = mod._ASSETS_QUERY.replace(
        "executionForLatestMaterialization { status timestamp } } }",
        "executionForLatestMaterialization { status timestamp } } }\n      ... on PythonError { message }")
    assert bad != mod._ASSETS_QUERY
    errors = graphql.validate(schema, graphql.parse(bad))
    assert any("can never be of type 'PythonError'" in e.message for e in errors)


def test_project_overview_reports_alert_policies_for_the_location(monkeypatch):
    monkeypatch.setattr("requests.request", FakeGithubApi(repo="acme/widgets", tree=[], contents={}).request)
    _pypi_github_release(monkeypatch)
    _osv(monkeypatch)
    tools, client = _overview_tools(None)
    client.alert_policies = [
        {"id": "p1", "name": "fail-alert", "description": "d", "enabled": True, "eventTypes": ["ASSET_MATERIALIZATION_FAILURE"]},
        {"id": "p2", "name": "off", "description": "", "enabled": False, "eventTypes": None},
    ]
    result = tools.call("project_overview", {"location": "ingestion"})
    assert "error" not in result, result
    assert result["alert_policy_count"] == 2
    assert result["alert_policies"][0] == {"id": "p1", "name": "fail-alert", "description": "d", "enabled": True,
                                           "event_types": ["ASSET_MATERIALIZATION_FAILURE"]}
    assert result["alert_policies"][1]["event_types"] == []
    alert_calls = [c for c in client.calls if "alertPoliciesForCodeLocation" in c[0]]
    assert [v for _, v in alert_calls] == [{"location": "ingestion"}]


def test_project_overview_survives_an_unreadable_alert_policies_field(monkeypatch):
    _pypi_github_release(monkeypatch)
    _osv(monkeypatch)
    tools, client = _overview_tools(None)
    client.alert_policies = mod.EvidenceToolError("Dagster+ GraphQL errors: not authorized")
    result = tools.call("project_overview", {"location": "ingestion"})
    assert "error" not in result, result
    assert result["alert_policies"] is None and result["alert_policy_count"] is None
    assert "not authorized" in result["alert_policies_error"]
    assert len(result["assets"]) == 1


def test_asset_checks_union_members_other_than_assetchecks_count_as_no_checks():
    for typename in ("AssetCheckNeedsMigrationError", "AssetCheckNeedsUserCodeUpgrade"):
        assert mod._check_health({"assetChecksOrError": {"__typename": typename}}) == {
            "passing": 0, "failing": 0, "stale": 0, "failing_checks": []}


# ── check_dagster_api ──────────────────────────────────────────────────────────────────────────────────────────────

def test_check_dagster_api_returns_a_signature_for_a_real_name_and_not_found_for_a_fake_one():
    tools, _ = _overview_tools(None)
    result = tools.call("check_dagster_api", {"names": ["dg.load_from_defs_folder", "dg.not_a_real_name"]})
    assert "error" not in result, result
    real = result["names"]["dg.load_from_defs_folder"]
    assert real["exists"] is True and real["signature"].startswith("(")
    assert result["names"]["dg.not_a_real_name"] == {"exists": False}
    assert result["dagster_version"]


def test_check_dagster_api_rejects_bad_input_and_is_allow_listable():
    tools, _ = _overview_tools(None)
    assert "requires `names`" in tools.call("check_dagster_api", {"names": []})["error"]
    assert "check_dagster_api" in tools.tool_names()
    restricted, _ = _overview_tools(None, allow=["project_overview"])
    assert "not enabled" in restricted.call("check_dagster_api", {"names": ["dg.Definitions"]})["error"]


def test_check_dagster_api_is_a_verbatim_copy_of_dagster_api_check_tools():
    import ast
    import pathlib

    def fn_source(path):
        text = path.read_text()
        node = next(n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name == "_check_dagster_api")
        return ast.get_source_segment(text, node)

    here = pathlib.Path(mod.__file__).resolve().parent
    assert fn_source(here / "component.py") == fn_source(here.parent / "dagster_api_check_tools" / "component.py")


# ── include_locations ──────────────────────────────────────────────────────────────────────────────────────────────

def test_include_locations_refuses_anything_outside_the_list_with_zero_http_calls(monkeypatch):
    monkeypatch.setattr("requests.request", _deny_http)
    monkeypatch.setattr("requests.get", _deny_http)
    monkeypatch.setattr("requests.post", _deny_http)
    tools, client = _overview_tools(None, include_locations=["a", "b"], exclude_locations=["b"])
    for name, needle in (("b", "excluded"), ("c", "not in include_locations")):
        result = tools.call("project_overview", {"location": name})
        assert needle in result["error"], result
    assert client.calls == []


def test_include_locations_allows_a_listed_location(monkeypatch):
    _pypi_github_release(monkeypatch)
    _osv(monkeypatch)
    tools, _ = _overview_tools(None, include_locations=["ingestion", "b"], exclude_locations=["b"])
    assert "error" not in tools.call("project_overview", {"location": "ingestion"})
