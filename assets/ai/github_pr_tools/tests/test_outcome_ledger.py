"""propose_fix/open_issue/review_pull_request write an outcome-ledger entry when ledger_path and
run_id are both set -- classified filed (a real write happened), would_file (practice mode),
refused (a guardrail declined it), or decision_needed (review_pull_request's APPROVE path refuses
because a person must decide, e.g. an 'Kind: improvement' PR). No ledger_path/run_id: a no-op, same
behavior as before this feature existed. Reuses FakeGithubApi/_tools/patched from
test_github_pr_tools.py -- only requests.request is mocked, everything else (classification,
ledger file writing) is exercised for real."""
import json

import pytest

from .conftest import load_component_module
from .test_github_pr_tools import FakeGithubApi, _FakeResp, _tools, patched  # noqa: F401


@pytest.fixture()
def mod():
    return load_component_module()


def _entries(ledger_dir, run_id):
    path = ledger_dir / f"{run_id}.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_no_ledger_path_writes_nothing(mod, patched, tmp_path):
    api = patched
    api.contents["src/a.py"] = "line1\n"
    tools = _tools(mod, api)  # ledger_path unset
    tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "line1", "new": "line2"}]}, run_id="run-1")
    assert not list(tmp_path.glob("*.jsonl"))


def test_no_run_id_writes_nothing(mod, patched, tmp_path):
    api = patched
    api.contents["src/a.py"] = "line1\n"
    tools = _tools(mod, api, ledger_path=str(tmp_path))
    tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "line1", "new": "line2"}]})  # no run_id
    assert not list(tmp_path.glob("*.jsonl"))


def test_propose_fix_filed(mod, patched, tmp_path):
    api = patched
    api.contents["src/a.py"] = "line1\n"
    tools = _tools(mod, api, ledger_path=str(tmp_path))
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "line1", "new": "line2"}]}, run_id="run-1")
    assert "error" not in result
    entries = _entries(tmp_path, "run-1")
    assert len(entries) == 1
    assert entries[0]["bucket"] == "filed"
    assert entries[0]["tool"] == "propose_fix"
    assert entries[0]["kind"] == "pull_request"


def test_propose_fix_refused_logs_reason(mod, patched, tmp_path):
    api = patched
    tools = _tools(mod, api, ledger_path=str(tmp_path))
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "pyproject.toml", "content": "x"}]}, run_id="run-2")
    assert "error" in result
    entries = _entries(tmp_path, "run-2")
    assert entries[0]["bucket"] == "refused"
    assert "protected" in entries[0]["reason"]


def test_propose_fix_practice_mode_logs_would_file(mod, patched, tmp_path, monkeypatch):
    api = patched
    api.contents["src/a.py"] = "line1\n"
    monkeypatch.setenv("PRACTICE", "1")
    tools = _tools(mod, api, ledger_path=str(tmp_path), practice_mode_env_var="PRACTICE")
    result = tools.call("propose_fix", {"branch": "x", "title": "t", "body": "b", "files": [{"path": "src/a.py", "old": "line1", "new": "line2"}]}, run_id="run-3")
    assert result.get("practice_mode") is True
    entries = _entries(tmp_path, "run-3")
    assert entries[0]["bucket"] == "would_file"
    assert entries[0]["title"] == "t"


def test_open_issue_filed(mod, patched, tmp_path):
    api = patched

    def _issues_handler(method, url, headers=None, timeout=None, **kwargs):
        path = url.split(f"/repos/{api.repo}", 1)[-1]
        if method == "POST" and path == "/labels":
            return _FakeResp(201, {"name": "engineering-agent"})
        if method == "POST" and path == "/issues":
            return _FakeResp(201, {"number": 99, "html_url": f"https://github.com/{api.repo}/issues/99"})
        return api.request(method, url, headers=headers, timeout=timeout, **kwargs)

    import unittest.mock
    with unittest.mock.patch("requests.request", _issues_handler):
        tools = _tools(mod, api, ledger_path=str(tmp_path))
        result = tools.call("open_issue", {"title": "Bug", "body": "Details"}, run_id="run-4")
    assert "error" not in result
    entries = _entries(tmp_path, "run-4")
    assert entries[0]["bucket"] == "filed"
    assert entries[0]["kind"] == "github_issue"


def test_review_pull_request_approve_refused_is_decision_needed(mod, patched, tmp_path):
    api = patched
    api.add_pull(1, body="Kind: improvement")
    api.add_file(1, "src/a.py")
    api.set_checks("abc123", {"build": "success"})
    tools = _tools(mod, api, ledger_path=str(tmp_path), reviewer_token="reviewer-token")
    result = tools.call("review_pull_request", {"number": 1, "decision": "APPROVE", "body": "lgtm"}, run_id="run-5")
    assert "error" in result
    entries = _entries(tmp_path, "run-5")
    assert entries[0]["bucket"] == "decision_needed"
    assert "Not approved" in entries[0]["reason"]


def test_read_only_tools_write_no_ledger_entry(mod, patched, tmp_path):
    api = patched
    api.add_pull(1)
    tools = _tools(mod, api, ledger_path=str(tmp_path))
    tools.call("get_pull_request", {"number": 1}, run_id="run-6")
    tools.call("list_open_pull_requests", {}, run_id="run-6")
    tools.call("assess_pull_request", {"number": 1}, run_id="run-6")
    assert not _entries(tmp_path, "run-6")
