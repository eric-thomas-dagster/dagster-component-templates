"""Committed tests for SlackPrApprovalWatcherComponent.

Mocks `requests.request` (GitHub) with a tiny in-memory fake, and `_get_slack_client` with a fake
client exposing conversations_history/reactions_get/chat_postMessage -- every other behavior
(vote classification, the second-identity review call, the checks-failing refusal, the
branch-prefix + approval + passing-checks merge gate, cursor-based resolved/last_error tracking)
is exercised for real.
"""
import json
import re

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
        self.text = json.dumps(body) if body is not None else ""
        self.content = b"1" if body is not None else b""

    def json(self):
        return self._body


class FakeGithubApi:
    def __init__(self, repo="acme/widgets"):
        self.repo = repo
        self.pulls = {}
        self.files = {}
        self.checks = {}
        self.reviews = {}
        self.refs = set()

    def add_pull(self, number, *, head_sha="sha1", head_ref="agent/fix-1", state="open", merged=False):
        self.pulls[number] = {"number": number, "html_url": "u", "title": "t", "body": "", "user": {"login": "bot"}, "state": state, "merged": merged, "head": {"sha": head_sha, "ref": head_ref}}
        self.files.setdefault(number, [{"filename": "src/a.py", "status": "modified", "additions": 1, "deletions": 1, "patch": ""}])
        self.checks.setdefault(head_sha, [])
        self.reviews.setdefault(number, [])
        self.refs.add(head_ref)

    def set_checks(self, sha, conclusions):
        self.checks[sha] = [{"name": n, "conclusion": c, "status": "completed"} for n, c in conclusions.items()]

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        path = url.split(f"/repos/{self.repo}", 1)[-1]
        auth = headers.get("Authorization") if headers else None

        m = re.match(r"^/pulls/(\d+)/files$", path)
        if method == "GET" and m:
            return _FakeResp(200, self.files.get(int(m.group(1)), []))
        m = re.match(r"^/pulls/(\d+)/reviews$", path)
        if method == "GET" and m:
            return _FakeResp(200, self.reviews.get(int(m.group(1)), []))
        if method == "POST" and m:
            number = int(m.group(1))
            body = kwargs["json"]
            login = {"Bearer reviewer-token": "reviewer-bot"}.get(auth, "primary-bot")
            self.reviews[number].append({"user": {"login": login}, "state": body["event"] if body["event"] != "COMMENT" else "COMMENTED"})
            return _FakeResp(200, {"id": 1})
        m = re.match(r"^/pulls/(\d+)/merge$", path)
        if method == "PUT" and m:
            number = int(m.group(1))
            self.pulls[number]["merged"] = True
            self.pulls[number]["state"] = "closed"
            return _FakeResp(200, {"merged": True})
        m = re.match(r"^/pulls/(\d+)$", path)
        if method == "GET" and m:
            number = int(m.group(1))
            if number not in self.pulls:
                return _FakeResp(404, {"message": "Not Found"})
            return _FakeResp(200, self.pulls[number])
        m = re.match(r"^/commits/([^/]+)/check-runs$", path)
        if method == "GET" and m:
            return _FakeResp(200, {"check_runs": self.checks.get(m.group(1), [])})
        m = re.match(r"^/commits/([^/]+)/statuses$", path)
        if method == "GET" and m:
            return _FakeResp(200, [])
        m = re.match(r"^/issues/(\d+)/comments$", path)
        if method == "GET" and m:
            return _FakeResp(200, [])
        m = re.match(r"^/pulls/(\d+)/update-branch$", path)
        if method == "PUT" and m:
            return _FakeResp(202, {"message": "Updating pull request branch."})
        m = re.match(r"^/git/refs/heads/(.+)$", path)
        if method == "DELETE" and m:
            self.refs.discard(m.group(1))
            return _FakeResp(204, None)
        raise AssertionError(f"FakeGithubApi: no handler for {method} {path}")


class FakeSlackClient:
    def __init__(self):
        self.messages = {}
        self.posted_replies = []

    def auth_test(self):
        return {"ok": True, "user_id": "U_BOT"}

    def users_info(self, user):
        return {"user": {"profile": {"display_name": user}}}

    def add_message(self, ts, payload):
        self.messages[ts] = {"metadata": {"event_type": "dagster_pr_review_v1", "event_payload": payload}, "reactions": {}}

    def vote(self, ts, emoji, user):
        self.messages[ts]["reactions"].setdefault(emoji, []).append(user)

    def conversations_history(self, channel, oldest=None, include_all_metadata=None, limit=None, cursor=None):
        return {"ok": True, "messages": [{"ts": ts, "metadata": m["metadata"]} for ts, m in self.messages.items()], "response_metadata": {"next_cursor": ""}}

    def reactions_get(self, channel, timestamp):
        m = self.messages[timestamp]
        reactions = [{"name": k, "users": v} for k, v in m["reactions"].items() if v]
        return {"ok": True, "message": {"reactions": reactions}}

    def chat_postMessage(self, channel, thread_ts=None, text=None):
        self.posted_replies.append({"channel": channel, "thread_ts": thread_ts, "text": text})
        return {"ok": True, "ts": "999.000"}


def _component(mod, **overrides):
    kwargs = dict(repo="acme/widgets", slack_channel="C1", reviewer_token_env_var="REVIEWER_TOKEN", reviewer_login="reviewer-bot", branch_prefix="agent/")
    kwargs.update(overrides)
    return mod.SlackPrApprovalWatcherComponent(**kwargs)


def _run(mod, monkeypatch, comp, github, slack, cursor=None, reviewer_token="reviewer-token"):
    monkeypatch.setenv("GITHUB_TOKEN", "primary-token")
    if reviewer_token:
        monkeypatch.setenv("REVIEWER_TOKEN", reviewer_token)
    else:
        monkeypatch.delenv("REVIEWER_TOKEN", raising=False)
    monkeypatch.setattr("requests.request", github.request)
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: slack)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    return sensor_def(dg.build_sensor_context(cursor=cursor))


def test_no_open_threads_skips(mod, monkeypatch):
    comp = _component(mod)
    result = _run(mod, monkeypatch, comp, FakeGithubApi(), FakeSlackClient())
    assert "no open PR-review" in result.skip_message


def test_approve_reaction_records_a_review_under_the_reviewer_identity(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "success"})
    slack.add_message("1.0", {"pr_number": 42, "approve_emoji": "white_check_mark", "reject_emoji": "x"})
    slack.vote("1.0", "white_check_mark", "U1")
    comp = _component(mod)
    result = _run(mod, monkeypatch, comp, github, slack)
    assert github.reviews[42][0]["user"]["login"] == "reviewer-bot"
    assert github.reviews[42][0]["state"] == "APPROVE"
    assert any("Approved on GitHub" in r["text"] for r in slack.posted_replies)
    assert "approved" in result.skip_reason.skip_message


def test_reject_reaction_requests_changes_and_resolves_the_thread(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "success"})
    slack.add_message("1.0", {"pr_number": 42, "approve_emoji": "white_check_mark", "reject_emoji": "x"})
    slack.vote("1.0", "x", "U1")
    comp = _component(mod)
    result = _run(mod, monkeypatch, comp, github, slack)
    assert github.reviews[42][0]["state"] == "REQUEST_CHANGES"
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


def test_approve_refused_while_checks_failing(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "failure"})
    slack.add_message("1.0", {"pr_number": 42, "approve_emoji": "white_check_mark", "reject_emoji": "x"})
    slack.vote("1.0", "white_check_mark", "U1")
    comp = _component(mod)
    result = _run(mod, monkeypatch, comp, github, slack)
    assert not github.reviews[42]
    assert any("CI is failing" in r["text"] for r in slack.posted_replies)
    # second tick: the same warning is not re-posted
    result2 = _run(mod, monkeypatch, comp, github, slack, cursor=result.cursor)
    assert len([r for r in slack.posted_replies if "CI is failing" in r["text"]]) == 1


def test_approve_refused_without_a_reviewer_token(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "success"})
    slack.add_message("1.0", {"pr_number": 42, "approve_emoji": "white_check_mark", "reject_emoji": "x"})
    slack.vote("1.0", "white_check_mark", "U1")
    comp = _component(mod)
    _run(mod, monkeypatch, comp, github, slack, reviewer_token=None)
    assert not github.reviews[42]
    assert any("second GitHub identity" in r["text"] for r in slack.posted_replies)


def test_auto_merge_merges_once_approved_and_checks_pass(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, head_ref="agent/fix-1")
    github.set_checks("sha1", {"build": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True)
    result = _run(mod, monkeypatch, comp, github, slack)
    assert github.pulls[42]["merged"] is True
    assert "agent/fix-1" not in github.refs
    assert any("Merged #42" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


def test_auto_merge_refuses_a_non_agent_branch(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, head_ref="someones-manual-branch")
    github.set_checks("sha1", {"build": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True)
    _run(mod, monkeypatch, comp, github, slack)
    assert github.pulls[42]["merged"] is False
    assert any("agent branches" in r["text"] for r in slack.posted_replies)


def test_already_closed_pr_is_resolved_without_a_review_call(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, state="closed", merged=True)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod)
    result = _run(mod, monkeypatch, comp, github, slack)
    assert not github.reviews[42]
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


def test_resolved_thread_is_not_rescanned(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, state="closed", merged=True)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod)
    first = _run(mod, monkeypatch, comp, github, slack)

    def _boom(*a, **k):
        raise AssertionError("should not re-fetch an already-resolved PR")
    monkeypatch.setattr(mod, "_gh_get_pull_request", _boom)
    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert "no open PR-review" in second.skip_message
