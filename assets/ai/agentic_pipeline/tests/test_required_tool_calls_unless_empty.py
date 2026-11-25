"""`required_tool_calls_unless_empty`: skip required_tool_calls enforcement when this step's own
source resolves to an empty list -- the "driven by an upstream per-item step's 0-item findings
list" shape, where there's nothing to call a tool ABOUT. Reported by Hooli: without this, an
empty-findings run fails (or needs the blunt on_required_tool_calls_missing: warn, which also
silences a run that legitimately should have called the tool). Only the low-level MCP
discover/call is faked, matching test_required_tool_calls.py's convention."""
import json
import logging
from types import SimpleNamespace

import pytest

from .conftest import new_state, tool_call

SERVER = [{"name": "svc", "type": "stdio", "command": ["x"]}]


@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    calls = []

    async def discover(log, servers):
        return [{"server": "svc", "name": "open_issue", "description": "", "input_schema": {"type": "object", "properties": {}}}]

    async def call(**kw):
        calls.append(kw["tool_name"])
        return {"raw": "ok", "value": {"ok": True}, "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return calls


def _ctx():
    return SimpleNamespace(log=logging.getLogger("agentic_test"))


def _base_step(**extra):
    return {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER,
        "required_tool_calls": ["svc__open_issue"], "required_tool_calls_unless_empty": True, **extra,
    }


def test_empty_source_skips_enforcement_cleanly(mod, fake_litellm, tool_use_loop_capture):
    """An empty upstream findings list ([]) -- the step finishes with no reminder, no failure,
    even though required_tool_calls is set and the tool was never called."""
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "nothing to do"}, id="f1")])

    state = new_state(text=json.dumps([]))
    out = mod._do_tool_use_loop(_base_step(), state, _ctx())

    assert out["text"] == "nothing to do"
    assert out["required_tool_calls_skipped_empty_source"] is True
    assert tool_use_loop_capture == []
    # no reminder was ever issued -- the first finalize call was accepted outright
    phases = [t["phase"] for t in out["tool_call_trace"]]
    assert "finalize_rejected" not in phases


def test_non_empty_source_still_enforces_normally(mod, fake_litellm, tool_use_loop_capture):
    """A real (non-empty) findings list -- required_tool_calls is enforced exactly as before:
    an early finalize without the tool call gets rejected once."""
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done early"}, id="f1")])
    fake_litellm.reply("", tool_calls=[tool_call("svc__open_issue", {}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f2")])

    state = new_state(text=json.dumps([{"title": "a finding"}]))
    out = mod._do_tool_use_loop(_base_step(), state, _ctx())

    assert out["text"] == "done"
    assert out["required_tool_calls_skipped_empty_source"] is False
    assert tool_use_loop_capture == ["open_issue"]
    phases = [t["phase"] for t in out["tool_call_trace"]]
    assert phases.count("finalize_rejected") == 1


def test_flag_off_still_fails_on_empty_source(mod, fake_litellm, tool_use_loop_capture):
    """Without required_tool_calls_unless_empty, an empty source gets no special treatment --
    the existing fail-by-default behavior is unchanged (this is an opt-in, not a new default)."""
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done early"}, id="f1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "still done"}, id="f2")])  # never calls it

    step = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER,
            "required_tool_calls": ["svc__open_issue"]}
    state = new_state(text=json.dumps([]))

    with pytest.raises(RuntimeError, match="required_tool_calls not satisfied"):
        mod._do_tool_use_loop(step, state, _ctx())
