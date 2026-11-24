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
    # A SensorResult (not a bare SkipReason) now, so the cursor can still carry
    # verifying state across ticks even when there's nothing pending to scan --
    # SensorResult wraps a string skip_reason in its own SkipReason object.
    assert "no open PR-review" in result.skip_reason.skip_message


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


def test_auto_merge_refreshes_other_open_agent_prs_by_default(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, head_ref="agent/fix-1")
    github.add_pull(43, head_ref="agent/fix-2")  # another open agent PR -- should get update-branch'd
    github.add_pull(44, head_ref="someones-manual-branch")  # a human's -- must NOT be touched
    github.set_checks("sha1", {"build": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True)
    _run(mod, monkeypatch, comp, github, slack)
    assert github.pulls[42]["merged"] is True
    assert github.updated_branches == [43]


def test_auto_merge_refresh_can_be_disabled(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, head_ref="agent/fix-1")
    github.add_pull(43, head_ref="agent/fix-2")
    github.set_checks("sha1", {"build": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True, refresh_other_agent_prs_after_merge=False)
    _run(mod, monkeypatch, comp, github, slack)
    assert github.pulls[42]["merged"] is True
    assert github.updated_branches == []


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


class FakeDagsterPlusApi:
    """A tiny fake of Dagster+'s GraphQL endpoint -- just the two operations
    verify_after_merge uses: launchRunReexecution and runOrError{status}."""

    def __init__(self):
        self.run_statuses = {}
        self.launched = []

    def post(self, url, json=None, headers=None, timeout=None):
        query = json["query"]
        if "launchRunReexecution" in query:
            parent = json["variables"]["p"]["parentRunId"]
            self.launched.append(parent)
            run_id = f"rerun-of-{parent[:8]}"
            self.run_statuses.setdefault(run_id, "STARTED")
            return _FakeResp(200, {"data": {"launchRunReexecution": {"__typename": "LaunchRunSuccess", "run": {"runId": run_id}}}})
        if "runOrError" in query:
            run_id = json["variables"]["id"]
            return _FakeResp(200, {"data": {"runOrError": {"status": self.run_statuses.get(run_id, "STARTED")}}})
        raise AssertionError(f"FakeDagsterPlusApi: no handler for query: {query[:80]}")


_FAILED_RUN_ID = "11111111-2222-3333-4444-555555555555"


def _merge_ready_pull(github, number=42, body=f"Fixes failed run {_FAILED_RUN_ID}."):
    github.add_pull(number, head_ref="agent/fix-1")
    github.pulls[number]["body"] = body
    github.set_checks("sha1", {"build": "success"})
    github.reviews[number] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]


def test_verify_after_merge_starts_a_reexecution_and_keeps_the_thread_open(mod, monkeypatch):
    github, slack, plus = FakeGithubApi(), FakeSlackClient(), FakeDagsterPlusApi()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    monkeypatch.setattr("requests.post", plus.post)
    comp = _component(mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod")
    result = _run(mod, monkeypatch, comp, github, slack)
    assert github.pulls[42]["merged"] is True
    assert plus.launched == [_FAILED_RUN_ID]
    assert any("Re-running" in r["text"] for r in slack.posted_replies)
    cur = json.loads(result.cursor)
    assert "1.0" not in cur["resolved_ts"]  # still open -- waiting on verification
    assert cur["verifying"]["1.0"]["failed_run"] == _FAILED_RUN_ID


def test_verify_after_merge_reports_success_on_a_later_tick(mod, monkeypatch):
    github, slack, plus = FakeGithubApi(), FakeSlackClient(), FakeDagsterPlusApi()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    monkeypatch.setattr("requests.post", plus.post)
    comp = _component(mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod")
    first = _run(mod, monkeypatch, comp, github, slack)

    plus.run_statuses[f"rerun-of-{_FAILED_RUN_ID[:8]}"] = "SUCCESS"
    # Nothing new pending -- the thread is resolved as far as PR state goes -- but the
    # verification must still be polled and reported.
    second = _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert any("Verified" in r["text"] and "succeeded" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(second.cursor)["resolved_ts"]
    assert json.loads(second.cursor)["verifying"] == {}


def test_verify_after_merge_reports_still_failing(mod, monkeypatch):
    github, slack, plus = FakeGithubApi(), FakeSlackClient(), FakeDagsterPlusApi()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    monkeypatch.setattr("requests.post", plus.post)
    comp = _component(mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod")
    first = _run(mod, monkeypatch, comp, github, slack)

    plus.run_statuses[f"rerun-of-{_FAILED_RUN_ID[:8]}"] = "FAILURE"
    _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert any("Still failing" in r["text"] for r in slack.posted_replies)


def test_verify_after_merge_times_out(mod, monkeypatch):
    github, slack, plus = FakeGithubApi(), FakeSlackClient(), FakeDagsterPlusApi()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    monkeypatch.setattr("requests.post", plus.post)
    comp = _component(mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod", verify_timeout_minutes=5)
    first = _run(mod, monkeypatch, comp, github, slack)

    # Backdate the tracked verification's start time well past the 5-minute timeout, rather
    # than faking time.time() globally -- simpler and just as real a test of the timeout path.
    cur = json.loads(first.cursor)
    cur["verifying"]["1.0"]["started"] -= 10 * 60
    second = _run(mod, monkeypatch, comp, github, slack, cursor=json.dumps(cur))
    assert any("did not finish" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(second.cursor)["resolved_ts"]


def test_verify_after_merge_with_no_failed_run_in_body_resolves_immediately(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    _merge_ready_pull(github, body="No run id mentioned here.")
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod")
    result = _run(mod, monkeypatch, comp, github, slack)
    assert any("does not name the failed run" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


def test_verify_after_merge_requires_dagster_url(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, auto_merge=True, verify_after_merge=True)  # dagster_url unset
    result = _run(mod, monkeypatch, comp, github, slack)
    assert any("dagster_url is not set" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


def test_post_merge_command_failure_skips_verification(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    _merge_ready_pull(github)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(
        mod, auto_merge=True, verify_after_merge=True, dagster_url="https://acme.dagster.cloud/prod",
        post_merge_command="false",  # the `false` shell builtin/binary always exits 1
    )
    result = _run(mod, monkeypatch, comp, github, slack)
    assert any("deploy step failed" in r["text"] for r in slack.posted_replies)
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


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
    assert "no open PR-review" in second.skip_reason.skip_message


def test_skips_in_branch_deployment_by_default(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, state="closed", merged=True)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod)
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    result = _run(mod, monkeypatch, comp, github, slack)
    assert isinstance(result, dg.SkipReason)
    assert "branch deployment" in result.skip_message


def test_skip_in_branch_deployments_false_still_runs(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, state="closed", merged=True)
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod, skip_in_branch_deployments=False)
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    result = _run(mod, monkeypatch, comp, github, slack)
    assert "1.0" in json.loads(result.cursor)["resolved_ts"]


# ── branch deployment link in the thread + the merge confirmation ───────────────────────────────────────────────────

BRANCH_URL = "https://acme.dagster.cloud/pr-42--agent-fix"


def _with_branch_deployment(github, number=42, files=None):
    github.comments[number] = [{"body": f"Your branch deployment is ready: {BRANCH_URL}"}]
    if files is not None:
        github.files[number] = files


def _links(slack):
    return [r for r in slack.posted_replies if "branch deployment of this change" in r["text"]]


def test_branch_deployment_link_is_posted_once_for_a_not_low_risk_pr(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "success"})
    _with_branch_deployment(github, files=[{"filename": ".github/workflows/ci.yml", "status": "modified", "additions": 1, "deletions": 0, "patch": ""}])
    slack.add_message("1.0", {"pr_number": 42})
    comp = _component(mod)
    first = _run(mod, monkeypatch, comp, github, slack)
    links = _links(slack)
    assert len(links) == 1 and links[0]["thread_ts"] == "1.0"
    assert links[0]["text"] == f":test_tube: A branch deployment of this change is ready to test: <{BRANCH_URL}>"
    assert "1.0" in json.loads(first.cursor)["branch_links"]
    # a later tick (cursor carried forward) does not repeat it
    _run(mod, monkeypatch, comp, github, slack, cursor=first.cursor)
    assert len(_links(slack)) == 1


def test_no_branch_deployment_link_for_a_low_risk_pr_or_when_there_is_no_deployment(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    github.set_checks("sha1", {"build": "success"})
    _with_branch_deployment(github)  # default fake file src/a.py, 2 lines, checks passing: low risk
    slack.add_message("1.0", {"pr_number": 42})
    _run(mod, monkeypatch, _component(mod), github, slack)
    assert _links(slack) == []
    # not low risk but no branch deployment comment: nothing to post
    risky, slack2 = FakeGithubApi(), FakeSlackClient()
    risky.add_pull(42)
    risky.files[42] = [{"filename": "Dockerfile", "status": "modified", "additions": 1, "deletions": 0, "patch": ""}]
    slack2.add_message("1.0", {"pr_number": 42})
    _run(mod, monkeypatch, _component(mod), risky, slack2)
    assert _links(slack2) == []


def test_branch_deployment_link_can_be_turned_off_and_retries_after_a_slack_failure(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42)
    _with_branch_deployment(github, files=[{"filename": "Dockerfile", "status": "modified", "additions": 1, "deletions": 0, "patch": ""}])
    slack.add_message("1.0", {"pr_number": 42})
    _run(mod, monkeypatch, _component(mod, post_branch_deployment_link=False), github, slack)
    assert _links(slack) == []
    real_post = slack.chat_postMessage
    slack.chat_postMessage = lambda **k: (_ for _ in ()).throw(RuntimeError("slack down"))
    failed = _run(mod, monkeypatch, _component(mod), github, slack)
    assert "1.0" not in json.loads(failed.cursor)["branch_links"]
    slack.chat_postMessage = real_post
    _run(mod, monkeypatch, _component(mod), github, slack, cursor=failed.cursor)
    assert len(_links(slack)) == 1


def test_merge_confirmation_names_the_pr_and_its_real_base_branch(mod, monkeypatch):
    github, slack = FakeGithubApi(), FakeSlackClient()
    github.add_pull(42, head_ref="agent/fix-1", base_ref="release/2.0")
    github.set_checks("sha1", {"build": "success"})
    github.reviews[42] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    slack.add_message("1.0", {"pr_number": 42})
    _run(mod, monkeypatch, _component(mod, auto_merge=True), github, slack)
    merged = [r for r in slack.posted_replies if "Merged" in r["text"]]
    assert [r["text"] for r in merged] == [":rocket: Merged #42 into release/2.0."]
    assert merged[0]["thread_ts"] == "1.0"
