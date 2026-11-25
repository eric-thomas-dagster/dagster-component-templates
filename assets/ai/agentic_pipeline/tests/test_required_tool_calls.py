"""`required_tool_calls`/`on_required_tool_calls_missing` on `tool_use_loop`: a tool the model
MUST call at least once before the loop is allowed to finish (by finalize, or by stopping
without a tool call) -- a model that drafts a finding and then says "filed" without having
actually called the filing tool is a real, repeatedly observed failure mode prompt wording
alone doesn't reliably prevent. Ported from litellm_agent's own required_tool_calls (same
semantics: one reminder, then fail/warn). Only the low-level MCP discover/call is faked,
matching test_skill_keys.py's `tool_use_loop_capture` convention."""
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
    return {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER,
            "required_tool_calls": ["svc__open_issue"], **extra}


def test_finalize_without_required_tool_gets_one_reminder_then_succeeds(mod, fake_litellm, tool_use_loop_capture):
    # 1) tries to finish immediately -- rejected, one reminder
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done early"}, id="f1")])
    # 2) now calls the required tool
    fake_litellm.reply("", tool_calls=[tool_call("svc__open_issue", {}, id="t1")])
    # 3) finishes for real -- required tool was attempted, no more rejection
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f2")])

    out = mod._do_tool_use_loop(_base_step(), new_state(), _ctx())

    assert out["text"] == "done"
    assert out["required_tools_missing"] == []
    assert tool_use_loop_capture == ["open_issue"]
    phases = [t["phase"] for t in out["tool_call_trace"]]
    assert phases.count("finalize_rejected") == 1
    assert phases.count("finalize") == 1


def test_no_tool_call_stop_point_also_gets_one_reminder(mod, fake_litellm, tool_use_loop_capture):
    # 1) plain text, no tool call at all -- rejected (same stop point as finalize, different trigger)
    fake_litellm.reply("here is my answer")
    # 2) calls the required tool
    fake_litellm.reply("", tool_calls=[tool_call("svc__open_issue", {}, id="t1")])
    # 3) plain text again -- now satisfied, allowed to stop
    fake_litellm.reply("done for real")

    out = mod._do_tool_use_loop(_base_step(), new_state(), _ctx())

    assert out["text"] == "done for real"
    assert out["required_tools_missing"] == []
    assert tool_use_loop_capture == ["open_issue"]


def test_still_missing_after_reminder_fails_by_default(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done early"}, id="f1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "still done"}, id="f2")])  # never calls it

    with pytest.raises(RuntimeError, match="required_tool_calls not satisfied"):
        mod._do_tool_use_loop(_base_step(), new_state(), _ctx())
    assert tool_use_loop_capture == []  # the required tool was never actually called


def test_still_missing_after_reminder_warns_instead_of_failing(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done early"}, id="f1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "still done"}, id="f2")])

    out = mod._do_tool_use_loop(_base_step(on_required_tool_calls_missing="warn"), new_state(), _ctx())

    assert out["text"] == "still done"
    assert out["required_tools_missing"] == ["svc__open_issue"]


def test_tool_called_up_front_needs_no_reminder(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("svc__open_issue", {}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    out = mod._do_tool_use_loop(_base_step(), new_state(), _ctx())

    assert out["text"] == "done"
    assert out["required_tools_missing"] == []
    phases = [t["phase"] for t in out["tool_call_trace"]]
    assert "finalize_rejected" not in phases  # never rejected -- it had already called the required tool


def test_without_required_tool_calls_nothing_changes(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    out = mod._do_tool_use_loop(_base_step(required_tool_calls=None), new_state(), _ctx())

    assert out["text"] == "done"
    assert "required_tools_missing" not in out
