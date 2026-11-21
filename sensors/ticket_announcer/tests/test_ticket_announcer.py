"""Committed tests for TicketAnnouncerComponent.

Mocks the GitHub/Linear list calls and the Slack client directly (component-level functions) --
every other behavior (cursor-based dedup, excerpt extraction, multi-source merging, partial
failure not blocking the rest) is exercised for real through the sensor's own evaluation function.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeSlackClient:
    def __init__(self, fail_for=()):
        self.posted = []
        self.fail_for = set(fail_for)

    def chat_postMessage(self, channel, text):
        if any(f in text for f in self.fail_for):
            raise RuntimeError("Slack is down")
        self.posted.append({"channel": channel, "text": text})
        return {"ok": True, "ts": "1.0"}


def _component(mod, **overrides):
    kwargs = dict(slack_channel="C1")
    kwargs.update(overrides)
    return mod.TicketAnnouncerComponent(**kwargs)


def _run(mod, monkeypatch, comp, slack, github_issues=None, linear_issues=None, cursor=None):
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("LINEAR_API_KEY", "tok")
    monkeypatch.setattr(mod, "_gh_list_agent_issues", lambda *a, **k: github_issues or [])
    monkeypatch.setattr(mod, "_linear_list_agent_issues", lambda *a, **k: linear_issues or [])
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: slack)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    return sensor_def(dg.build_sensor_context(cursor=cursor))


def test_requires_at_least_one_source(mod):
    with pytest.raises(ValueError, match="repo"):
        mod.TicketAnnouncerComponent(slack_channel="C1")


def test_github_issue_is_announced_once(mod, monkeypatch):
    comp = _component(mod, repo="acme/widgets")
    slack = FakeSlackClient()
    issues = [{"number": 7, "title": "Flaky asset", "url": "https://github.com/acme/widgets/issues/7", "body": "Analysis\nit's flaky.\nSuggested change\nadd retries."}]

    first = _run(mod, monkeypatch, comp, slack, github_issues=issues)
    assert len(slack.posted) == 1
    assert "|7 Flaky asset>" in slack.posted[0]["text"]
    assert "it's flaky" in slack.posted[0]["text"]
    assert "Suggested change" not in slack.posted[0]["text"]

    second = _run(mod, monkeypatch, comp, slack, github_issues=issues, cursor=first.cursor)
    assert len(slack.posted) == 1  # not re-announced
    assert "no new tickets" in second.skip_reason.skip_message


def test_linear_issue_is_announced_once(mod, monkeypatch):
    comp = _component(mod, linear_team_id="team-1")
    slack = FakeSlackClient()
    issues = [{"number": "ENG-12", "title": "Deprecated API", "url": "https://linear.app/x/ENG-12", "body": "some finding"}]
    first = _run(mod, monkeypatch, comp, slack, linear_issues=issues)
    assert len(slack.posted) == 1
    assert "ENG-12" in slack.posted[0]["text"]

    second = _run(mod, monkeypatch, comp, slack, linear_issues=issues, cursor=first.cursor)
    assert len(slack.posted) == 1


def test_both_sources_announced_together(mod, monkeypatch):
    comp = _component(mod, repo="acme/widgets", linear_team_id="team-1")
    slack = FakeSlackClient()
    gh = [{"number": 1, "title": "A", "url": "u1", "body": "b1"}]
    lin = [{"number": "ENG-1", "title": "B", "url": "u2", "body": "b2"}]
    result = _run(mod, monkeypatch, comp, slack, github_issues=gh, linear_issues=lin)
    assert len(slack.posted) == 2
    assert json.loads(result.cursor) == ["GitHub:1", "Linear:ENG-1"]


def test_excerpt_falls_back_to_the_start_when_no_analysis_section(mod):
    text = mod._excerpt("just a plain body with no marker words at all", limit=10)
    assert text == "just a pla…"


def test_slack_failure_for_one_ticket_does_not_block_the_others_and_is_retried(mod, monkeypatch):
    comp = _component(mod, repo="acme/widgets")
    slack = FakeSlackClient(fail_for=["Bad one"])
    issues = [
        {"number": 1, "title": "Bad one", "url": "u1", "body": "b1"},
        {"number": 2, "title": "Good one", "url": "u2", "body": "b2"},
    ]
    result = _run(mod, monkeypatch, comp, slack, github_issues=issues)
    assert len(slack.posted) == 1
    assert json.loads(result.cursor) == ["GitHub:2"]  # #1 not marked announced, will retry next tick


def test_missing_github_token_reports_a_problem_without_crashing(mod, monkeypatch):
    comp = _component(mod, repo="acme/widgets")
    slack = FakeSlackClient()
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: slack)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert "GITHUB_TOKEN" in result.skip_reason.skip_message
