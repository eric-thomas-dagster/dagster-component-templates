"""Committed tests for TeamsPrApprovalWatcherComponent.

Mocks `requests.request` (GitHub) with a tiny in-memory fake, and `requests.get`/`requests.post`
(Microsoft Graph) with a tiny in-memory fake exposing the channel-messages listing (with Adaptive
Card attachments), per-message reactions, and reply posting -- every other behavior (vote
classification, the second-identity review call, the checks-failing refusal, the branch-prefix +
approval + passing-checks merge gate, cursor-based resolved/last_error tracking) is exercised for
real through the sensor's own evaluation function.
"""
import json
import re
from datetime import datetime, timezone

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

    def raise_for_status(self):
        pass


class FakeGithubApi:
    def __init__(self, repo="acme/widgets"):
        self.repo = repo
        self.pulls = {}
        self.files = {}
        self.checks = {}
        self.reviews = {}
        self.refs = set()
        self.updated_branches = []
        self.comments = {}

    def add_pull(self, number, *, head_sha="sha1", head_ref="agent/fix-1", state="open", merged=False, base_ref="main"):
        self.pulls[number] = {"number": number, "html_url": "u", "title": "t", "body": "", "user": {"login": "bot"}, "state": state, "merged": merged, "head": {"sha": head_sha, "ref": head_ref}, "base": {"ref": base_ref}}
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
            return _FakeResp(200, self.comments.get(int(m.group(1)), []))
        m = re.match(r"^/pulls/(\d+)/update-branch$", path)
        if method == "PUT" and m:
            self.updated_branches.append(int(m.group(1)))
            return _FakeResp(202, {"message": "Updating pull request branch."})
        if method == "GET" and path == "/pulls":
            return _FakeResp(200, [p for p in self.pulls.values() if p["state"] == "open"])
        m = re.match(r"^/git/refs/heads/(.+)$", path)
        if method == "DELETE" and m:
            self.refs.discard(m.group(1))
            return _FakeResp(204, None)
        raise AssertionError(f"FakeGithubApi: no handler for {method} {path}")


class FakeTeamsApi:
    """Fakes Microsoft Graph: OAuth token, channel-messages listing (Adaptive Card attachments
    carry the PR-review payload), per-message GET (reactions inline), and reply posting."""

    def __init__(self):
        self.messages = {}  # id -> {attachments, reactions}
        self.replies = []

    def add_message(self, mid, payload, reactions=None):
        card = {"_dagster_pr_review": payload}
        self.messages[mid] = {
            "id": mid,
            "createdDateTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.0000000Z"),
            "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": json.dumps(card)}],
            "reactions": reactions or [],
        }

    def react(self, mid, reaction_type, user_id):
        self.messages[mid]["reactions"].append({"reactionType": reaction_type, "user": {"user": {"id": user_id}}})

    def get(self, url, headers=None, timeout=None):
        if url.endswith("/messages?$top=50") or "/messages?$top=50" in url:
            return _FakeResp(200, {"value": list(self.messages.values())})
        m = re.search(r"/messages/([^/]+)$", url)
        if m:
            return _FakeResp(200, self.messages[m.group(1)])
        raise AssertionError(f"FakeTeamsApi.get: no handler for {url}")

    def post(self, url, headers=None, json=None, data=None, timeout=None):
        if "oauth2/v2.0/token" in url:
            return _FakeResp(200, {"access_token": "BEARER123"})
        if url.endswith("/replies"):
            self.replies.append(json)
            return _FakeResp(200, {"id": "reply1"})
        raise AssertionError(f"FakeTeamsApi.post: no handler for {url}")


def _component(mod, **overrides):
    kwargs = dict(
        repo="acme/widgets", teams_team_id="T1", teams_channel_id="Ch1",
        reviewer_token_env_var="REVIEWER_TOKEN", reviewer_login="reviewer-bot",
    )
    kwargs.update(overrides)
    return mod.TeamsPrApprovalWatcherComponent(**kwargs)


def _run(mod, monkeypatch, comp, github, teams, cursor=None):
    import requests
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("REVIEWER_TOKEN", "reviewer-token")
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")
    monkeypatch.setattr(requests, "request", github.request)
    monkeypatch.setattr(requests, "get", teams.get)
    monkeypatch.setattr(requests, "post", teams.post)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    ctx = dg.build_sensor_context(cursor=cursor)
    return sensor_def(ctx)


def test_approve_reaction_records_a_github_review(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1")
    github.set_checks("sha1", {"ci": "success"})
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry", "approver_allowlist": ["aad-1"]})
    teams.react("m1", "like", "aad-1")

    result = _run(mod, monkeypatch, comp, github, teams)
    assert github.reviews[42][0]["state"] == "APPROVE"  # the review event name, as GithubPrScopeError-free POST records it
    assert any("approved" in a for a in result.skip_reason.skip_message.split(";"))
    assert teams.replies and "Approved" in teams.replies[0]["body"]["content"]


def test_nonallowlisted_reaction_is_ignored(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1")
    github.set_checks("sha1", {"ci": "success"})
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry", "approver_allowlist": ["aad-1"]})
    teams.react("m1", "like", "some-other-user")  # not on the allowlist

    result = _run(mod, monkeypatch, comp, github, teams)
    assert github.reviews[42] == []
    assert not teams.replies


def test_reject_reaction_requests_changes(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1")
    github.set_checks("sha1", {"ci": "success"})
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry", "approver_allowlist": ["aad-1"]})
    teams.react("m1", "angry", "aad-1")

    result = _run(mod, monkeypatch, comp, github, teams)
    assert github.reviews[42][0]["state"] == "REQUEST_CHANGES"
    assert "Changes requested" in teams.replies[0]["body"]["content"]


def test_checks_failing_refuses_approval(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1")
    github.set_checks("sha1", {"ci": "failure"})
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry", "approver_allowlist": ["aad-1"]})
    teams.react("m1", "like", "aad-1")

    result = _run(mod, monkeypatch, comp, github, teams)
    assert github.reviews[42] == []
    assert any("CI is failing" in r["body"]["content"] for r in teams.replies)


def test_auto_merge_after_approval_and_passing_checks(mod, monkeypatch):
    comp = _component(mod, auto_merge=True)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1", head_ref="agent/fix-1")
    github.set_checks("sha1", {"ci": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]  # already approved by the reviewer identity
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry", "approver_allowlist": ["aad-1"]})

    result = _run(mod, monkeypatch, comp, github, teams)
    assert github.pulls[42]["merged"] is True
    assert any("Merged" in r["body"]["content"] for r in teams.replies)


def test_pr_no_longer_open_marks_resolved_and_is_not_rescanned(mod, monkeypatch):
    comp = _component(mod)
    github = FakeGithubApi()
    github.add_pull(42, head_sha="sha1", state="closed", merged=True)
    teams = FakeTeamsApi()
    teams.add_message("m1", {"pr_number": 42, "approve_emoji": "like", "reject_emoji": "angry"})

    first = _run(mod, monkeypatch, comp, github, teams)
    assert json.loads(first.cursor)["resolved_ids"] == ["m1"]

    teams.react("m1", "like", "aad-1")  # even if someone reacts afterwards
    second = _run(mod, monkeypatch, comp, github, teams, cursor=first.cursor)
    assert github.reviews[42] == []  # never re-examined


def test_missing_github_token_skips(mod, monkeypatch):
    comp = _component(mod)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result, dg.SkipReason)
    assert "GITHUB_TOKEN" in result.skip_message


def test_teams_auth_failure_skips(mod, monkeypatch):
    comp = _component(mod)
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.delenv("TEAMS_TENANT_ID", raising=False)
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result, dg.SkipReason)
    assert "Teams auth failed" in result.skip_message


def test_skip_in_branch_deployment_by_default(mod, monkeypatch):
    comp = _component(mod)
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    defs = comp.build_defs(None)
    [sensor_def] = defs.sensors
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result, dg.SkipReason)
    assert "branch deployment" in result.skip_message
