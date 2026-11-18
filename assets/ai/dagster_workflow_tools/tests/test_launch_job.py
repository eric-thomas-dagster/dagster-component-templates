"""launch_job: the non-blocking alternative to run_job/materialize_assets, via the real
`launchRun` GraphQL mutation (confirmed directly against dagster-graphql's own schema, not
guessed) -- and the per-requester authorization check that's the whole reason this tool is
separate from run_job rather than a flag on it. Mocks only `requests.post` (the unavoidable
external call), never the authorization logic itself."""
import pytest


GRAPHQL_URL = "http://localhost:3000/graphql"
REPO_LOCATION = "my_location"


def _tools(mod, instance, defs_module_spec, **kw):
    return mod.DagsterWorkflowTools(
        instance, definitions_module=defs_module_spec,
        graphql_url=GRAPHQL_URL, repository_location_name=REPO_LOCATION, **kw,
    )


def _install_fake_graphql(monkeypatch, *, typename="LaunchRunSuccess", run_id="r-123", errors=None):
    import requests

    calls = []

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            if errors:
                return {"errors": errors}
            data = {"__typename": typename}
            if typename == "LaunchRunSuccess":
                data["run"] = {"runId": run_id}
            else:
                data["message"] = "boom"
            return {"data": {"launchRun": data}}

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return _Resp()

    monkeypatch.setattr(requests, "post", fake_post)
    return calls


# --- authorization is the point ---------------------------------------------------------------

def test_requester_who_owns_the_job_can_launch_it(mod, instance, defs_module_spec, monkeypatch):
    calls = _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert result["success"] is True
    assert result["run_id"] == "r-123"
    assert result["url"] == "http://localhost:3000/runs/r-123"
    # returned immediately -- exactly one HTTP call, no polling/sleeping
    assert len(calls) == 1
    variables = calls[0][1]["json"]["variables"]
    assert variables["executionParams"]["selector"] == {
        "jobName": "named_job", "repositoryName": "__repository__", "repositoryLocationName": REPO_LOCATION,
    }


def test_requester_who_does_not_own_the_job_is_refused(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="intruder@co.com")
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "error" in result
    assert "not an owner" in result["error"]


def test_asset_job_with_an_unowned_asset_fails_closed(mod, instance, defs_module_spec, monkeypatch):
    """unowned_job IS an asset job (covers `boom`), just one where no asset declares an
    owner -- _job_owners correctly returns [] (determined, zero owners), not None, so this hits
    the ordinary not-an-owner refusal, same as any other owners check with no matches."""
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "unowned_job"})
    assert "not an owner" in result["error"] and "owners: []" in result["error"]


def test_job_with_no_discoverable_owners_source_fails_closed(mod, instance, defs_module_spec, monkeypatch):
    """A job name that resolves to nothing (or a non-asset job with no job_owners entry) has NO
    owners source at all -- _job_owners returns None, not [], and the refusal says so plainly."""
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "does_not_exist"})
    assert "no owners configured" in result["error"]


def test_explicit_job_owners_authorizes_a_non_asset_target(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="jane@co.com", job_owners={"unowned_job": ["jane@co.com"]})
    result = tools.call("launch_job", {"job_name": "unowned_job"})
    assert result["success"] is True


def test_team_prefixed_owner_never_authorizes_a_real_requester(mod, instance, defs_module_spec, monkeypatch):
    """A `team:<name>` owner entry has no group-membership check behind it (same rule as
    slack_run_request_sensor/teams_run_request_sensor) -- a real requester identity is always an
    email or a platform user id, which can never equal the literal string 'team:...', so this
    entry is effectively unreachable. Not special-cased in code; just a consequence of exact
    string matching against an identity shape no real sender ever has."""
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="jane@co.com", job_owners={"unowned_job": ["team:data-platform"]})
    result = tools.call("launch_job", {"job_name": "unowned_job"})
    assert "not an owner" in result["error"]


def test_requester_missing_entirely_refuses_without_ever_calling_graphql(mod, instance, defs_module_spec, monkeypatch):
    calls = _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec)  # no requester=
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "no requester identity" in result["error"]
    assert calls == []


def test_allow_jobs_still_applies_before_authorization(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch)
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com", allow_jobs=["some_other_job"])
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "not in allow_jobs" in result["error"]


def test_missing_graphql_config_refuses_clearly(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "graphql_url and repository_location_name" in result["error"]


# --- GraphQL call shape ------------------------------------------------------------------------

def test_graphql_failure_response_surfaces_as_a_tool_error(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch, typename="PythonError")
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "launchRun failed" in result["error"]


def test_graphql_top_level_errors_surface_as_a_tool_error(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch, errors=[{"message": "not authorized"}])
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com")
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "launchRun GraphQL request error" in result["error"]


def test_api_token_env_var_sent_as_header(mod, instance, defs_module_spec, monkeypatch):
    calls = _install_fake_graphql(monkeypatch)
    monkeypatch.setenv("MY_DAGSTER_TOKEN", "secret-token-value")
    tools = _tools(mod, instance, defs_module_spec, requester="owner@co.com", graphql_api_token_env_var="MY_DAGSTER_TOKEN")
    tools.call("launch_job", {"job_name": "named_job"})
    assert calls[0][1]["headers"]["Dagster-Cloud-Api-Token"] == "secret-token-value"


def test_run_url_omitted_when_graphql_url_not_set(mod, instance, defs_module_spec, monkeypatch):
    _install_fake_graphql(monkeypatch)
    tools = mod.DagsterWorkflowTools(
        instance, definitions_module=defs_module_spec,
        repository_location_name=REPO_LOCATION, requester="owner@co.com",
    )
    # graphql_url unset entirely -> the clear "requires graphql_url" refusal, not a crash
    result = tools.call("launch_job", {"job_name": "named_job"})
    assert "graphql_url and repository_location_name" in result["error"]
