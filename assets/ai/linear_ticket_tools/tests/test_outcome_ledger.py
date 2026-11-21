"""create_issue writes an outcome-ledger entry when ledger_path and run_id are both set -- same
shape/rationale as github_pr_tools' own ledger. Reuses FakeLinearApi/_tools/patched from
test_linear_ticket_tools.py -- only requests.post is mocked."""
import json
import os

import pytest

from .conftest import load_component_module
from .test_linear_ticket_tools import FakeLinearApi, _tools, patched  # noqa: F401


@pytest.fixture()
def mod():
    return load_component_module()


def _entries(ledger_dir, run_id):
    path = ledger_dir / f"{run_id}.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_no_ledger_path_writes_nothing(mod, patched, tmp_path):
    tools = _tools(mod, patched)
    tools.call("create_issue", {"title": "Bug", "body": "Details"}, run_id="run-1")
    assert not list(tmp_path.glob("*.jsonl"))


def test_no_run_id_writes_nothing(mod, patched, tmp_path):
    tools = _tools(mod, patched, ledger_path=str(tmp_path))
    tools.call("create_issue", {"title": "Bug", "body": "Details"})  # no run_id
    assert not list(tmp_path.glob("*.jsonl"))


def test_create_issue_filed(mod, patched, tmp_path):
    tools = _tools(mod, patched, ledger_path=str(tmp_path))
    result = tools.call("create_issue", {"title": "Bug", "body": "Details"}, run_id="run-2")
    assert "error" not in result
    entries = _entries(tmp_path, "run-2")
    assert len(entries) == 1
    assert entries[0]["bucket"] == "filed"
    assert entries[0]["kind"] == "linear_ticket"


def test_create_issue_practice_mode_logs_would_file(mod, patched, tmp_path):
    tools = _tools(mod, patched, ledger_path=str(tmp_path), practice_mode_env_var="ENG_PRACTICE_LEDGER")
    os.environ["ENG_PRACTICE_LEDGER"] = "practice"
    try:
        result = tools.call("create_issue", {"title": "Bug", "body": "Details"}, run_id="run-3")
    finally:
        del os.environ["ENG_PRACTICE_LEDGER"]
    assert result["practice_mode"] is True
    entries = _entries(tmp_path, "run-3")
    assert entries[0]["bucket"] == "would_file"


def test_find_issues_writes_no_ledger_entry(mod, patched, tmp_path):
    tools = _tools(mod, patched, ledger_path=str(tmp_path))
    tools.call("find_issues", {"query": "something_specific_enough"}, run_id="run-4")
    assert not _entries(tmp_path, "run-4")
