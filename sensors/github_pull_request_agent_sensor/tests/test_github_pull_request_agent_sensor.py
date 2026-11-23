"""Committed tests for GithubPullRequestAgentSensorComponent.

Mocks `requests.request` (GitHub) with a tiny in-memory fake, and `_get_slack_client` with a fake
client recording posted messages/metadata/reactions -- every other behavior (cursor-based
dedup-by-head-sha, cursor pruning, RunRequest tags/run_key, message formatting) is exercised for
real through the sensor's own evaluation function.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)
        self.content = b"1"

    def json(self):
        return self._body


class FakeGithubPulls:
    def __init__(self, repo="acme/widgets"):
        self.repo = repo
        self.pulls = []

    def add(self, number, head_sha, title="Fix bug", head_ref="agent/fix", author="bot"):
        self.pulls = [p for p in self.pulls if p["number"] != number]
        self.pulls.append({
            "number": number, "html_url": f"https://github.com/{self.repo}/pull/{number}", "title": title,
            "user": {"login": author}, "head": {"sha": head_sha, "ref": head_ref},
        })

    def remove(self, number):
        self.pulls = [p for p in self.pulls if p["number"] != number]

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        assert method == "GET" and url.endswith("/pulls")
        return _FakeResp(200, list(self.pulls))


class FakeSlackClient:
    def __init__(self):
        self.posted = []
        self._n = 0

    def chat_postMessage(self, channel, text, metadata=None, thread_ts=None):
        self._n += 1
        ts = f"{self._n}.000"
        self.posted.append({"channel": channel, "text": text, "metadata": metadata, "ts": ts})
        return {"ok": True, "ts": ts, "channel": channel}

    def reactions_add(self, channel, timestamp, name):
        return {"ok": True}


def _component(mod, **overrides):
    kwargs = dict(
        sensor_name="gh_pr_sensor", asset_keys=["pr_reviewer"], repo="acme/widgets",
        slack_channel="C1", minimum_interval_seconds=60,
    )
    kwargs.update(overrides)
    return mod.GithubPullRequestAgentSensorComponent(**kwargs)


def _run(mod, monkeypatch, comp, github, slack, cursor=None):
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setattr("requests.request", github.request)
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: slack)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    ctx = dg.build_sensor_context(cursor=cursor)
    return sensor_def(ctx)


def test_new_pr_posts_to_slack_and_launches_a_run(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubPulls()
    github.add(42, "abcdef123456", title="Fix the thing")
    slack = FakeSlackClient()
    result = _run(mod, monkeypatch, comp, github, slack)

    assert len(slack.posted) == 1
    assert "#42" in slack.posted[0]["text"]
    assert slack.posted[0]["metadata"]["event_type"] == "dagster_pr_review_v1"
    assert slack.posted[0]["metadata"]["event_payload"]["pr_number"] == 42

    [rr] = result.run_requests
    assert rr.run_key == "pr-42-abcdef1"
    assert rr.tags["eng/pr_number"] == "42"
    assert rr.tags["slack/thread_ts"] == slack.posted[0]["ts"]
    assert rr.asset_selection == [dg.AssetKey("pr_reviewer")]


def test_unchanged_pr_is_not_reposted(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubPulls()
    github.add(42, "abcdef123456")
    slack = FakeSlackClient()
    first = _run(mod, monkeypatch, comp, github, slack)

    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert len(slack.posted) == 1
    assert isinstance(second, dg.SensorResult) and not second.run_requests


def test_new_head_commit_reposts_and_relaunches(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubPulls()
    github.add(42, "abcdef123456")
    slack = FakeSlackClient()
    first = _run(mod, monkeypatch, comp, github, slack)

    github.add(42, "newsha7890ab")
    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert len(slack.posted) == 2
    [rr] = second.run_requests
    assert rr.run_key == "pr-42-newsha7"


def test_closed_pr_is_pruned_from_cursor(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubPulls()
    github.add(42, "abcdef123456")
    slack = FakeSlackClient()
    first = _run(mod, monkeypatch, comp, github, slack)
    assert json.loads(first.cursor) == {"42": "abcdef123456"}

    github.remove(42)
    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert isinstance(second, dg.SensorResult) and not second.run_requests
    assert json.loads(second.cursor) == {}


def test_approver_allowlist_is_embedded_when_set(mod, monkeypatch):
    comp = _component(mod, approver_allowlist=["U1", "U2"])
    github = FakeGithubPulls()
    github.add(1, "sha0000001")
    slack = FakeSlackClient()
    _run(mod, monkeypatch, comp, github, slack)
    assert slack.posted[0]["metadata"]["event_payload"]["approver_allowlist"] == ["U1", "U2"]


def test_missing_github_token_skips(mod, monkeypatch):
    comp = _component(mod)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result, dg.SkipReason)
    assert "GITHUB_TOKEN" in result.skip_message


def test_non_agent_branch_pr_is_never_announced_or_launched(mod, monkeypatch):
    comp = _component(mod)  # default branch_prefix="agent/"
    github = FakeGithubPulls()
    github.add(1, "shaagent01", head_ref="agent/fix-1")
    github.add(2, "shahuman02", head_ref="feature/some-human-branch")
    github.add(3, "shabot00003", head_ref="dependabot/pip/foo-1.2.3")
    slack = FakeSlackClient()
    result = _run(mod, monkeypatch, comp, github, slack)

    assert len(slack.posted) == 1
    assert "#1" in slack.posted[0]["text"]
    [rr] = result.run_requests
    assert rr.tags["eng/pr_number"] == "1"
    assert json.loads(result.cursor) == {"1": "shaagent01"}


def test_empty_branch_prefix_watches_every_pr(mod, monkeypatch):
    comp = _component(mod, branch_prefix="")
    github = FakeGithubPulls()
    github.add(1, "shaagent01", head_ref="agent/fix-1")
    github.add(2, "shahuman02", head_ref="feature/some-human-branch")
    slack = FakeSlackClient()
    result = _run(mod, monkeypatch, comp, github, slack)

    assert len(slack.posted) == 2
    assert len(result.run_requests) == 2


def test_backlog_is_capped_per_tick_and_picked_up_later(mod, monkeypatch):
    comp = _component(mod, announcements_per_tick=2)
    github = FakeGithubPulls()
    for n in range(1, 5):
        github.add(n, f"sha{n:08d}", head_ref="agent/fix")
    slack = FakeSlackClient()
    first = _run(mod, monkeypatch, comp, github, slack)

    assert len(slack.posted) == 2
    assert len(first.run_requests) == 2
    assert len(json.loads(first.cursor)) == 2

    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert len(slack.posted) == 4
    assert len(second.run_requests) == 2
    assert len(json.loads(second.cursor)) == 4


def test_skips_in_branch_deployment_by_default(mod, monkeypatch):
    comp = _component(mod)
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result, dg.SkipReason)
    assert "branch deployment" in result.skip_message


def test_skip_in_branch_deployments_false_still_runs(mod, monkeypatch):
    comp = _component(mod, skip_in_branch_deployments=False)
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    github = FakeGithubPulls()
    github.add(1, "shaagent01", head_ref="agent/fix-1")
    slack = FakeSlackClient()
    result = _run(mod, monkeypatch, comp, github, slack)
    assert len(result.run_requests) == 1
