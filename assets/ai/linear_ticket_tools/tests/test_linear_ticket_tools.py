"""Real, committed tests for LinearTicketTools / LinearTicketToolsComponent.

Mocks ONLY `requests.post` (the one HTTP call every Linear-scope function makes -- Linear has no
REST surface, GraphQL is the only way in) via a small in-memory fake -- every other behavior
(generic-word filtering, label lookup/creation fallback, practice mode, the call()/t_* dispatch,
error shaping) is exercised for real.
"""
import json

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class FakeLinearApi:
    """A tiny in-memory Linear GraphQL API: one team, its labels, and its issues."""

    def __init__(self, team_id="team-1"):
        self.team_id = team_id
        self.labels = []  # [{"id", "name"}]
        self.issues = []  # [{"identifier", "title", "url", "description"}]
        self.label_creation_allowed = True
        self.calls = []
        self._next_issue_num = 1

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(json)
        query = json["query"]
        variables = json.get("variables") or {}

        if "labels(first:100)" in query:
            return _FakeResp(200, {"data": {"team": {"labels": {"nodes": list(self.labels)}}}})
        if "issueLabelCreate" in query:
            if not self.label_creation_allowed:
                return _FakeResp(200, {"errors": [{"message": "forbidden"}]})
            label = {"id": f"label-{len(self.labels) + 1}", "name": variables["i"]["name"]}
            self.labels.append(label)
            return _FakeResp(200, {"data": {"issueLabelCreate": {"issueLabel": {"id": label["id"]}}}})
        if "issueCreate" in query:
            inp = variables["i"]
            number = self._next_issue_num
            self._next_issue_num += 1
            identifier = f"ENG-{number}"
            issue = {"identifier": identifier, "title": inp["title"], "url": f"https://linear.app/acme/issue/{identifier}", "description": inp["description"]}
            self.issues.append(issue)
            return _FakeResp(200, {"data": {"issueCreate": {"success": True, "issue": {k: issue[k] for k in ("identifier", "title", "url")}}}})
        if "issues(first:20" in query:
            # find_issues: OR of per-word title-contains filters, over issues carrying the marker
            words = [f["title"]["containsIgnoreCase"].lower() for f in variables["f"]]
            matches = [
                i for i in self.issues
                if "Filed by an engineering agent." in i["description"]
                and any(w in i["title"].lower() for w in words)
            ]
            return _FakeResp(200, {"data": {"issues": {"nodes": [{"identifier": i["identifier"], "title": i["title"], "url": i["url"]} for i in matches]}}})
        if "issues(first:50" in query:
            matches = [i for i in self.issues if "Filed by an engineering agent." in i["description"]]
            return _FakeResp(200, {"data": {"issues": {"nodes": list(matches)}}})
        raise AssertionError(f"FakeLinearApi: no handler for query: {query[:120]}")


def _tools(mod, api, **overrides):
    kwargs = dict(team_id=api.team_id, api_key="fake-key")
    kwargs.update(overrides)
    return mod.LinearTicketTools(**kwargs)


@pytest.fixture()
def patched(monkeypatch):
    api = FakeLinearApi()
    monkeypatch.setattr("requests.post", api.post)
    return api


def test_create_issue_labels_and_marks_the_description(mod, patched):
    api = patched
    tools = _tools(mod, api)
    result = tools.call("create_issue", {"title": "Flaky retry bug", "body": "Details here."})
    assert result["title"] == "Flaky retry bug"
    assert len(api.issues) == 1
    assert "Filed by an engineering agent." in api.issues[0]["description"]
    assert api.labels and api.labels[0]["name"] == "engineering-agent"


def test_create_issue_still_works_when_label_creation_is_forbidden(mod, patched):
    """A personal API key may lack permission to create labels -- filing still succeeds, just
    without a label, since duplicate-detection keys off the description marker, not a label."""
    api = patched
    api.label_creation_allowed = False
    tools = _tools(mod, api)
    result = tools.call("create_issue", {"title": "Flaky retry bug", "body": "Details here."})
    assert result["title"] == "Flaky retry bug"
    assert len(api.issues) == 1
    assert not api.labels


def test_create_issue_practice_mode_files_nothing(mod, patched):
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE_LINEAR")
    import os
    os.environ["ENG_PRACTICE_LINEAR"] = "practice"
    try:
        result = tools.call("create_issue", {"title": "Flaky retry bug", "body": "Details here."})
    finally:
        del os.environ["ENG_PRACTICE_LINEAR"]
    assert result["practice_mode"] is True
    assert result["would_open"] == "linear_ticket"
    assert not api.issues


def test_find_issues_filters_generic_words_and_matches_by_title(mod, patched):
    api = patched
    tools = _tools(mod, api)
    tools.call("create_issue", {"title": "flaky_retry_bug in orders", "body": "x"})
    result = tools.call("find_issues", {"query": "improve this dagster asset's flaky_retry_bug reliability"})
    assert len(result["issues"]) == 1
    assert result["issues"][0]["title"] == "flaky_retry_bug in orders"


def test_find_issues_returns_empty_for_only_generic_words(mod, patched):
    """When every word is generic/too-short, no search is even sent -- zero results, not an error."""
    api = patched
    tools = _tools(mod, api)
    result = tools.call("find_issues", {"query": "improve reliability"})
    assert result["issues"] == []
    assert api.calls == []


def test_list_agent_issues_only_returns_marker_tagged_issues(mod, patched):
    api = patched
    api.issues.append({"identifier": "ENG-1", "title": "Agent filed this", "url": "u", "description": "Filed by an engineering agent."})
    api.issues.append({"identifier": "ENG-2", "title": "A human filed this", "url": "u2", "description": "no marker here"})
    tools = _tools(mod, api)
    result = tools.call("list_agent_issues", {})
    assert [i["number"] for i in result["issues"]] == ["ENG-1"]


def test_call_unknown_tool(mod, patched):
    tools = _tools(mod, patched)
    result = tools.call("delete_everything", {})
    assert "error" in result and "unknown tool" in result["error"]


def test_call_tool_not_in_allowlist(mod, patched):
    tools = _tools(mod, patched, allow=["find_issues"])
    result = tools.call("create_issue", {"title": "t", "body": "b"})
    assert "error" in result and "not enabled" in result["error"]


def test_constructing_with_unknown_allow_entry_raises(mod, patched):
    with pytest.raises(ValueError, match="unknown tool"):
        _tools(mod, patched, allow=["not_a_real_tool"])


def test_graphql_error_surfaces_as_a_clear_error_not_an_exception(mod, patched, monkeypatch):
    def _broken_post(url, json=None, headers=None, timeout=None):
        return _FakeResp(200, {"errors": [{"message": "team not found"}]})
    monkeypatch.setattr("requests.post", _broken_post)
    tools = _tools(mod, patched)
    result = tools.call("list_agent_issues", {})
    assert "error" in result and "team not found" in result["error"]
