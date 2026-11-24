"""Committed tests for GithubPrTools / GithubPrToolsComponent.

Mocks ONLY `requests.request` (the one HTTP call every GitHub-PR-scope function makes) via a small
in-memory fake GitHub API -- every other behavior (risk assessment, the second-identity APPROVE
gate, propose_fix's protected-path/duplicate-PR guards, the call()/t_* dispatch, error shaping) is
exercised for real.
"""
import base64
import json
import re

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body) if not isinstance(body, str) else body
        self.content = b"1" if body is not None else b""

    def json(self):
        return self._body


class FakeGithubApi:
    """A tiny in-memory GitHub REST API: PRs, files, checks, reviews, contents, refs."""

    def __init__(self, repo="acme/widgets"):
        self.repo = repo
        self.calls = []
        self.pulls = {}
        self.files = {}
        self.checks = {}
        self.reviews = {}
        self.contents = {}
        self.refs = {"heads/main": "basesha123"}
        self.comments = {}

    def add_pull(self, number, *, title="Fix bug", author="bot", head_sha="abc123", head_ref="agent/fix-1", state="open", merged=False, body=""):
        self.pulls[number] = {
            "number": number, "html_url": f"https://github.com/{self.repo}/pull/{number}", "title": title, "body": body,
            "user": {"login": author}, "state": state, "merged": merged,
            "head": {"sha": head_sha, "ref": head_ref},
        }
        self.files.setdefault(number, [])
        self.checks.setdefault(head_sha, [])
        self.reviews.setdefault(number, [])

    def add_file(self, number, path, additions=1, deletions=1, status="modified"):
        self.files[number].append({"filename": path, "status": status, "additions": additions, "deletions": deletions, "patch": "@@ -1 +1 @@"})

    def set_checks(self, sha, conclusions):
        self.checks[sha] = [{"name": n, "conclusion": c, "status": "completed"} for n, c in conclusions.items()]

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        self.calls.append((method, url, headers.get("Authorization") if headers else None, kwargs))
        path = url.split(f"/repos/{self.repo}", 1)[-1]

        m = re.match(r"^/pulls/(\d+)/files$", path)
        if method == "GET" and m:
            return _FakeResp(200, self.files.get(int(m.group(1)), []))
        m = re.match(r"^/pulls/(\d+)/reviews$", path)
        if method == "GET" and m:
            return _FakeResp(200, self.reviews.get(int(m.group(1)), []))
        if method == "POST" and m:
            number = int(m.group(1))
            body = kwargs.get("json", {})
            login = {"Bearer reviewer-token": "reviewer-bot", "Bearer primary-token": "author-bot"}.get(headers.get("Authorization"), "someone")
            self.reviews.setdefault(number, []).append({"user": {"login": login}, "state": body["event"] if body["event"] != "COMMENT" else "COMMENTED"})
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
        if method == "GET" and path == "/pulls":
            return _FakeResp(200, list(self.pulls.values()))
        if method == "POST" and path == "/pulls":
            body = kwargs["json"]
            number = max(self.pulls) + 1 if self.pulls else 1
            self.add_pull(number, title=body["title"], body=body["body"], head_ref=body["head"])
            return _FakeResp(201, self.pulls[number])
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
            return _FakeResp(202, {"message": "Updating pull request branch."})
        m = re.match(r"^/git/ref/heads/(.+)$", path)
        if method == "GET" and m:
            sha = self.refs.get(f"heads/{m.group(1)}")
            if not sha:
                return _FakeResp(404, {"message": "Not Found"})
            return _FakeResp(200, {"object": {"sha": sha}})
        if method == "POST" and path == "/git/refs":
            body = kwargs["json"]
            ref_name = body["ref"].replace("refs/", "")
            self.refs[ref_name] = body["sha"]
            return _FakeResp(201, {"ref": body["ref"]})
        m = re.match(r"^/git/refs/heads/(.+)$", path)
        if method == "DELETE" and m:
            self.refs.pop(f"heads/{m.group(1)}", None)
            return _FakeResp(204, None)
        m = re.match(r"^/contents/(.+)$", path)
        if method == "GET" and m:
            p = m.group(1)
            if p not in self.contents:
                return _FakeResp(404, {"message": "Not Found"})
            return _FakeResp(200, {"sha": "filesha", "content": base64.b64encode(self.contents[p].encode()).decode()})
        if method == "PUT" and m:
            p = m.group(1)
            body = kwargs["json"]
            self.contents[p] = base64.b64decode(body["content"]).decode()
            return _FakeResp(201, {"content": {"sha": "newsha"}})
        raise AssertionError(f"FakeGithubApi: no handler for {method} {path}")


def _tools(mod, api, **overrides):
    kwargs = dict(repo=api.repo, token="primary-token", reviewer_token=None, protected_paths=(".github/", "pyproject.toml"), low_risk_paths=("src/", "tests/"), low_risk_max_files=2, low_risk_max_lines=30, max_files_per_fix=5)
    kwargs.update(overrides)
    return mod.GithubPrTools(**kwargs)


@pytest.fixture()
def patched(monkeypatch):
    api = FakeGithubApi()
    monkeypatch.setattr("requests.request", api.request)
    return api


def test_get_pull_request_reports_checks_and_files(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=5, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    pr = tools.t_get_pull_request(1)
    assert pr["checks"]["state"] == "passing"
    assert pr["files"][0]["path"] == "src/a.py"


def test_list_open_pull_requests(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_pull(2)
    tools = _tools(mod, api)
    result = tools.t_list_open_pull_requests()
    assert {p["number"] for p in result["pull_requests"]} == {1, 2}


def test_assess_pull_request_low_risk(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=5, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "low"
    assert risk["reasons"] == []


def test_assess_pull_request_needs_human_for_protected_path(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, ".github/workflows/ci.yml", additions=2, deletions=0)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "needs_human"
    assert "protected path" in risk["reasons"][0]


def test_assess_pull_request_needs_human_when_checks_failing(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=1, deletions=1)
    api.set_checks("abc123", {"build": "failure"})
    tools = _tools(mod, api)
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "needs_human"
    assert any("checks are failing" in r for r in risk["reasons"])


def test_assess_pull_request_protected_path_matches_inside_a_monorepo_subfolder(mod, patched):
    """protected_paths=('.github/', ...) must match a project living in a subfolder (e.g.
    'hooli-basics/.github/workflows/ci.yml'), not only a path rooted at the repo root -- a real
    gap reported from a monorepo deployment."""
    api = patched
    api.add_pull(1)
    api.add_file(1, "hooli-basics/.github/workflows/ci.yml", additions=2, deletions=0)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "needs_human"
    assert "protected path" in risk["reasons"][0]


def test_assess_pull_request_low_risk_path_matches_inside_a_monorepo_subfolder(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "hooli-basics/src/orders.py", additions=1, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "low"


def test_assess_pull_request_filename_pattern_matches_at_any_depth(mod, patched):
    """'Dockerfile' (no trailing '/') is filename-like -- must match the basename at any depth,
    not only a root-level 'Dockerfile'."""
    api = patched
    api.add_pull(1)
    api.add_file(1, "hooli-basics/services/worker/Dockerfile", additions=1, deletions=0)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, protected_paths=(".github/", "pyproject.toml", "Dockerfile"))
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "needs_human"
    assert "protected path" in risk["reasons"][0]


# ── exclude_paths: can neither read nor change (the agents' own repo, when self-hosted) ──────────

def test_exclude_paths_hides_matching_files_from_get_pull_request(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=1, deletions=1)
    api.add_file(1, "assets/ai/github_pr_tools/component.py", additions=3, deletions=0)
    tools = _tools(mod, api, exclude_paths=("assets/ai/github_pr_tools/",))
    pr = tools.t_get_pull_request(1)
    assert [f["path"] for f in pr["files"]] == ["src/a.py"]


def test_exclude_paths_keeps_assess_pull_request_from_seeing_the_excluded_file(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "assets/ai/github_pr_tools/component.py", additions=500, deletions=500)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, exclude_paths=("assets/ai/github_pr_tools/",))
    risk = tools.t_assess_pull_request(1)
    assert risk["level"] == "low"
    assert risk["reasons"] == []


def test_exclude_paths_refuses_propose_fix_on_the_excluded_file(mod, patched):
    api = patched
    tools = _tools(mod, api, exclude_paths=("assets/ai/github_pr_tools/",))
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "assets/ai/github_pr_tools/component.py", "content": "x"}]})
    assert "error" in result and "excluded" in result["error"]


def test_review_pull_request_comment_always_allowed(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, ".github/x.yml")
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    result = tools.t_review_pull_request(1, "COMMENT", "looks fine")
    assert result["decision"] == "COMMENT"


def test_review_pull_request_approve_refused_above_low_risk(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, ".github/x.yml")
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, reviewer_token="reviewer-token")
    result = tools.call("review_pull_request", {"number": 1, "decision": "APPROVE", "body": "lgtm"})
    assert "error" in result and "Not approved" in result["error"]


def test_review_pull_request_approve_refused_without_reviewer_token(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=1, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, reviewer_token=None)
    result = tools.call("review_pull_request", {"number": 1, "decision": "APPROVE", "body": "lgtm"})
    assert "error" in result and "second GitHub identity" in result["error"]


def test_review_pull_request_approve_uses_the_reviewer_identity(mod, patched):
    api = patched
    api.add_pull(1)
    api.add_file(1, "src/a.py", additions=1, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, reviewer_token="reviewer-token")
    result = tools.t_review_pull_request(1, "APPROVE", "lgtm")
    assert result["decision"] == "APPROVE"
    assert api.reviews[1][0]["user"]["login"] == "reviewer-bot"


def test_propose_fix_applies_a_targeted_edit(mod, patched):
    api = patched
    api.contents["src/a.py"] = "line1\nline2\nline3\n"
    tools = _tools(mod, api)
    result = tools.t_propose_fix("fix-1", "Fix a.py", "body", [{"path": "src/a.py", "old": "line2", "new": "line2-fixed"}])
    assert result["branch"] == "agent/fix-1"
    assert api.contents["src/a.py"] == "line1\nline2-fixed\nline3\n"


def test_propose_fix_refuses_a_protected_path(mod, patched):
    api = patched
    tools = _tools(mod, api)
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "pyproject.toml", "content": "x"}]})
    assert "error" in result and "protected" in result["error"]


def test_propose_fix_refuses_too_many_files(mod, patched):
    api = patched
    tools = _tools(mod, api, max_files_per_fix=1)
    files = [{"path": "src/a.py", "content": "x"}, {"path": "src/b.py", "content": "y"}]
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": files})
    assert "error" in result and "between 1 and 1 files" in result["error"]


def test_propose_fix_refuses_when_old_text_is_not_unique(mod, patched):
    api = patched
    api.contents["src/a.py"] = "dup\ndup\n"
    tools = _tools(mod, api)
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "dup", "new": "fixed"}]})
    assert "error" in result and "exactly once" in result["error"]


def test_propose_fix_refuses_when_an_open_pr_already_touches_the_file(mod, patched):
    api = patched
    api.add_pull(1, head_ref="agent/other-fix")
    api.add_file(1, "src/a.py")
    api.contents["src/a.py"] = "x\n"
    tools = _tools(mod, api)
    result = tools.call("propose_fix", {"branch": "y", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "x", "new": "y"}]})
    assert "error" in result and "already changes" in result["error"]


def test_call_unknown_tool(mod, patched):
    tools = _tools(mod, patched)
    result = tools.call("delete_everything", {})
    assert "error" in result and "unknown tool" in result["error"]


def test_call_tool_not_in_allowlist(mod, patched):
    tools = _tools(mod, patched, allow=["get_pull_request"])
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": []})
    assert "error" in result and "not enabled" in result["error"]


def test_merge_if_ready_requires_branch_prefix(mod, patched):
    api = patched
    api.add_pull(1, head_ref="someones-manual-branch")
    tools = _tools(mod, api)
    with pytest.raises(mod.GithubPrScopeError, match="agent branches"):
        mod._gh_merge_if_ready(api.repo, 1, branch_prefix="agent/", reviewer_login="reviewer-bot", token="primary-token", api_base_url="https://api.github.com")


def test_merge_if_ready_succeeds_when_approved_and_passing(mod, patched):
    api = patched
    api.add_pull(1, head_ref="agent/fix-1")
    api.set_checks("abc123", {"build": "success"})
    api.reviews[1] = [{"user": {"login": "reviewer-bot"}, "state": "APPROVED"}]
    result = mod._gh_merge_if_ready(api.repo, 1, branch_prefix="agent/", reviewer_login="reviewer-bot", token="primary-token", api_base_url="https://api.github.com")
    assert result["merged"] is True
    assert "heads/agent/fix-1" not in api.refs


def test_get_pull_request_includes_branch_deployment_url_when_posted_in_a_comment(mod, patched):
    """branch_deployment_url is read from PR comments/check-runs/commit-statuses, folded directly
    into get_pull_request's own result -- no separate 'post the link' tool needed: an agent (or a
    person) reading get_pull_request already sees it."""
    api = patched
    api.add_pull(1)
    api.set_checks("abc123", {"build": "success"})
    api.comments = {1: [{"body": "Deployed to https://acme.dagster.cloud/branch/my-feature--abc123 for review."}]}
    tools = _tools(mod, api)
    result = tools.call("get_pull_request", {"number": 1})
    assert result["branch_deployment_url"] == "https://acme.dagster.cloud/branch/my-feature--abc123"


def test_get_pull_request_branch_deployment_url_is_none_when_nothing_posted(mod, patched):
    api = patched
    api.add_pull(1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    result = tools.call("get_pull_request", {"number": 1})
    assert result["branch_deployment_url"] is None


def test_assess_pull_request_needs_human_for_kind_improvement(mod, patched):
    """A PR marked Kind: improvement always needs a person, even if it's otherwise small/low-risk --
    an improvement changes behavior, which a deterministic size/path check alone can't vet."""
    api = patched
    api.add_pull(1, body="What this does.\n\n---\nOpened by an engineering agent. Kind: improvement.")
    api.add_file(1, "src/a.py", additions=2, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    result = tools.call("assess_pull_request", {"number": 1})
    assert result["level"] == "needs_human"
    assert any("improvement" in r for r in result["reasons"])


def test_assess_pull_request_low_risk_fix_is_unaffected_by_the_kind_rule(mod, patched):
    api = patched
    api.add_pull(1, body="What this does.\n\n---\nOpened by an engineering agent. Kind: fix.")
    api.add_file(1, "src/a.py", additions=2, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api)
    result = tools.call("assess_pull_request", {"number": 1})
    assert result["level"] == "low"


def test_propose_fix_practice_mode_opens_nothing(mod, patched):
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE")
    import os
    os.environ["ENG_PRACTICE"] = "practice"
    try:
        result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "content": "y"}]})
    finally:
        del os.environ["ENG_PRACTICE"]
    assert result["practice_mode"] is True
    assert result["would_open"] == "pull_request"
    assert not api.pulls  # nothing was actually opened


def test_propose_fix_practice_mode_still_refuses_a_protected_path(mod, patched):
    """Practice mode withholds the real PR creation, not the validation that would have refused it anyway."""
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE")
    import os
    os.environ["ENG_PRACTICE"] = "practice"
    try:
        result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "pyproject.toml", "content": "x"}]})
    finally:
        del os.environ["ENG_PRACTICE"]
    assert "error" in result and "protected" in result["error"]


def test_propose_fix_is_not_practice_mode_when_env_var_unset(mod, patched):
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE_UNSET")
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "content": "y"}]})
    assert "practice_mode" not in result
    assert api.pulls


def test_propose_fix_practice_mode_treats_a_numeric_string_as_falsy_like_settings_shadow(mod, patched):
    """Mirrors the engineering-agents reference's own practice-mode vocabulary (a word, not '1') --
    any of '', '0', 'false', 'no', 'off' (case-insensitive) is treated as OFF."""
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE_OFF")
    import os
    os.environ["ENG_PRACTICE_OFF"] = "0"
    try:
        result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "content": "y"}]})
    finally:
        del os.environ["ENG_PRACTICE_OFF"]
    assert "practice_mode" not in result
    assert api.pulls


def test_refresh_open_agent_prs_updates_agent_branches_only(mod, patched):
    api = patched
    api.add_pull(1, head_ref="agent/fix-1")
    api.add_pull(2, head_ref="someones-manual-branch")
    tools = _tools(mod, api)
    result = tools.call("refresh_open_agent_prs", {})
    assert result["updated"] == [1]


# ── find_issues / open_issue (duplicate-check + practice mode) ──────────────────────────────────────

class _FakeSearchResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


def test_find_issues_filters_generic_words_and_searches_by_remaining_words(mod, patched, monkeypatch):
    api = patched
    captured_queries = []

    def _fake_get(url, params=None, headers=None, timeout=None):
        captured_queries.append(params["q"])
        if "flaky_retry_bug" in params["q"]:
            return _FakeSearchResp(200, {"items": [{"number": 7, "title": "flaky_retry_bug in orders", "html_url": "https://github.com/acme/widgets/issues/7", "state": "open", "closed_at": None}]})
        return _FakeSearchResp(200, {"items": []})

    monkeypatch.setattr("requests.get", _fake_get)
    tools = _tools(mod, api)
    result = tools.call("find_issues", {"query": "improve this dagster asset's flaky_retry_bug reliability"})
    assert result["issues"] == [{"number": 7, "title": "flaky_retry_bug in orders", "url": "https://github.com/acme/widgets/issues/7", "state": "open", "closed_at": None}]
    assert all("label:engineering-agent" in q for q in captured_queries)
    # generic words (dagster/asset/improve/reliability) are dropped -- only the specific word is searched.
    # is:open is no longer in the query -- a recently-closed match is now filtered client-side instead
    # (see test_find_issues_includes_a_recently_closed_duplicate_but_not_an_old_one below), not server-side.
    assert captured_queries == ["repo:acme/widgets is:issue label:engineering-agent flaky_retry_bug in:title"]


def test_find_issues_returns_empty_when_nothing_matches(mod, patched, monkeypatch):
    monkeypatch.setattr("requests.get", lambda *a, **k: _FakeSearchResp(200, {"items": []}))
    tools = _tools(mod, patched)
    result = tools.call("find_issues", {"query": "something_specific_enough"})
    assert result["issues"] == []


def test_find_issues_includes_a_recently_closed_duplicate_but_not_an_old_one(mod, patched, monkeypatch):
    """Hooli feedback: a person closing a bad finding (wontfix, already handled) is a signal not
    to re-file it -- find_issues must still surface that closed issue within the lookback window,
    not just still-open ones."""
    import datetime as _dt

    recent_closed_at = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_closed_at = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _fake_get(url, params=None, headers=None, timeout=None):
        return _FakeSearchResp(200, {"items": [
            {"number": 1, "title": "flaky_retry_bug open one", "html_url": "https://github.com/acme/widgets/issues/1", "state": "open", "closed_at": None},
            {"number": 2, "title": "flaky_retry_bug closed recently", "html_url": "https://github.com/acme/widgets/issues/2", "state": "closed", "closed_at": recent_closed_at},
            {"number": 3, "title": "flaky_retry_bug closed long ago", "html_url": "https://github.com/acme/widgets/issues/3", "state": "closed", "closed_at": old_closed_at},
        ]})

    monkeypatch.setattr("requests.get", _fake_get)
    tools = _tools(mod, patched, duplicate_check_lookback_days=30)
    result = tools.call("find_issues", {"query": "flaky_retry_bug"})
    numbers = {i["number"] for i in result["issues"]}
    assert numbers == {1, 2}  # the 90-day-old closed issue is excluded, the 5-day-old one is not


def test_find_issues_duplicate_check_lookback_days_is_configurable(mod, patched, monkeypatch):
    import datetime as _dt

    closed_at = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    monkeypatch.setattr("requests.get", lambda *a, **k: _FakeSearchResp(200, {"items": [
        {"number": 1, "title": "flaky_retry_bug", "html_url": "https://github.com/acme/widgets/issues/1", "state": "closed", "closed_at": closed_at},
    ]}))
    tools = _tools(mod, patched, duplicate_check_lookback_days=3)
    result = tools.call("find_issues", {"query": "flaky_retry_bug"})
    assert result["issues"] == []  # 10 days ago is outside a 3-day lookback


def test_open_issue_creates_a_labeled_issue(mod, patched):
    api = patched

    def _issues_handler(method, url, headers=None, timeout=None, **kwargs):
        path = url.split(f"/repos/{api.repo}", 1)[-1]
        if method == "POST" and path == "/labels":
            return _FakeResp(201, {"name": "engineering-agent"})
        if method == "POST" and path == "/issues":
            body = kwargs["json"]
            assert body["labels"] == ["engineering-agent"]
            assert "Filed by an engineering agent." in body["body"]
            return _FakeResp(201, {"number": 99, "html_url": f"https://github.com/{api.repo}/issues/99"})
        return api.request(method, url, headers=headers, timeout=timeout, **kwargs)

    import unittest.mock
    with unittest.mock.patch("requests.request", _issues_handler):
        tools = _tools(mod, api)
        result = tools.call("open_issue", {"title": "Flaky retry bug", "body": "Details here."})
    assert result == {"number": 99, "url": f"https://github.com/{api.repo}/issues/99"}


def test_open_issue_practice_mode_files_nothing(mod, patched):
    api = patched
    tools = _tools(mod, api, practice_mode_env_var="ENG_PRACTICE_ISSUE")
    import os
    os.environ["ENG_PRACTICE_ISSUE"] = "practice"
    try:
        result = tools.call("open_issue", {"title": "Flaky retry bug", "body": "Details here."})
    finally:
        del os.environ["ENG_PRACTICE_ISSUE"]
    assert result["practice_mode"] is True
    assert result["would_open"] == "github_issue"


# -- restrict_to_branch_prefix (off by default: this toolset also works as a general-purpose PR reviewer) -------
def test_restrict_to_branch_prefix_off_by_default_reads_any_pr(mod, patched):
    api = patched
    api.add_pull(1, head_ref="feature/a-humans-branch")
    tools = _tools(mod, api)
    pr = tools.t_get_pull_request(1)
    assert pr["number"] == 1


def test_restrict_to_branch_prefix_refuses_a_non_agent_pr(mod, patched):
    api = patched
    api.add_pull(1, head_ref="feature/a-humans-branch")
    tools = _tools(mod, api, restrict_to_branch_prefix=True)
    result = tools.call("get_pull_request", {"number": 1})
    assert "error" in result
    assert "not a 'agent/' branch" in result["error"]


def test_restrict_to_branch_prefix_allows_an_agent_pr(mod, patched):
    api = patched
    api.add_pull(1, head_ref="agent/fix-1")
    tools = _tools(mod, api, restrict_to_branch_prefix=True)
    pr = tools.t_get_pull_request(1)
    assert pr["number"] == 1


def test_restrict_to_branch_prefix_filters_list_open_pull_requests(mod, patched):
    api = patched
    api.add_pull(1, head_ref="agent/fix-1")
    api.add_pull(2, head_ref="feature/a-humans-branch")
    tools = _tools(mod, api, restrict_to_branch_prefix=True)
    result = tools.call("list_open_pull_requests")
    assert [p["number"] for p in result["pull_requests"]] == [1]


def test_restrict_to_branch_prefix_blocks_assess_and_review(mod, patched):
    api = patched
    api.add_pull(1, head_ref="feature/a-humans-branch")
    api.add_file(1, "src/a.py", additions=1, deletions=1)
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, restrict_to_branch_prefix=True)
    assert "error" in tools.call("assess_pull_request", {"number": 1})
    result = tools.call("review_pull_request", {"number": 1, "decision": "COMMENT", "body": "hi"})
    assert "error" in result
    assert not api.reviews[1]
