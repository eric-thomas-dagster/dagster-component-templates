"""`allowed_tools` on `tool_use_loop` must accept BOTH the bare tool name (`read_file`) and the
server-namespaced name the model actually sees and `required_tool_calls` already matches against
(`svc__read_file`) -- litellm_agent's own `allowed_tools` already uses the namespaced form (with
globs). Before this fix, a namespaced entry matched nothing (bare-name `in` check only), silently
shrinking the effective tool list to zero with no warning -- a real, reported Hooli bug. Only the
low-level MCP discover/call is faked, matching test_required_tool_calls.py's convention."""
import logging
from types import SimpleNamespace

import pytest

from .conftest import new_state, tool_call

SERVER = [{"name": "svc", "type": "stdio", "command": ["x"]}]


@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    calls = []

    async def discover(log, servers):
        return [
            {"server": "svc", "name": "read_file", "description": "", "input_schema": {"type": "object", "properties": {}}},
            {"server": "svc", "name": "list_files", "description": "", "input_schema": {"type": "object", "properties": {}}},
        ]

    async def call(**kw):
        calls.append(kw["tool_name"])
        return {"raw": "ok", "value": {"ok": True}, "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return calls


def _ctx():
    return SimpleNamespace(log=logging.getLogger("agentic_test"))


def _base_step(**extra):
    return {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER, **extra}


def test_namespaced_allowed_tools_entry_matches(mod, fake_litellm, tool_use_loop_capture):
    """The exact Hooli-reported shape: allowed_tools written in the namespaced form (consistent
    with how they already write required_tool_calls / litellm_agent's allowed_tools) must
    actually expose that tool, not silently produce zero."""
    fake_litellm.reply("", tool_calls=[tool_call("svc__read_file", {}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    out = mod._do_tool_use_loop(_base_step(allowed_tools=["svc__read_file"]), new_state(), _ctx())

    assert out["text"] == "done"
    assert tool_use_loop_capture == ["read_file"]


def test_bare_allowed_tools_entry_still_matches(mod, fake_litellm, tool_use_loop_capture):
    """The bare form keeps working too -- both forms are accepted."""
    fake_litellm.reply("", tool_calls=[tool_call("svc__read_file", {}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    out = mod._do_tool_use_loop(_base_step(allowed_tools=["read_file"]), new_state(), _ctx())

    assert out["text"] == "done"
    assert tool_use_loop_capture == ["read_file"]


def test_glob_pattern_matches_namespaced_form(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("svc__list_files", {}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    out = mod._do_tool_use_loop(_base_step(allowed_tools=["svc__list_*"]), new_state(), _ctx())

    assert out["text"] == "done"
    assert tool_use_loop_capture == ["list_files"]


def test_unmatched_allowed_tools_entry_fails_loudly_by_default(mod, fake_litellm, tool_use_loop_capture):
    """A typo'd or wrong-form entry matching zero discovered tools must error at step start,
    naming the unmatched entry and the real discovered tool names -- not silently run the loop
    with an empty effective tool list."""
    with pytest.raises(ValueError, match=r"allowed_tools.*matched zero discovered tools") as exc_info:
        mod._do_tool_use_loop(_base_step(allowed_tools=["svc__nonexistent_tool"]), new_state(), _ctx())

    assert "read_file" in str(exc_info.value)
    assert "list_files" in str(exc_info.value)
    # the loop never even started -- no litellm call was made
    assert fake_litellm.calls == []


def test_unmatched_allowed_tools_entry_warns_when_configured(mod, fake_litellm, tool_use_loop_capture, caplog):
    """on_allowed_tools_unmatched: warn' logs instead of raising, and the loop still runs with
    whatever (if anything) did match."""
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    with caplog.at_level(logging.WARNING):
        out = mod._do_tool_use_loop(
            _base_step(allowed_tools=["svc__nonexistent_tool"], on_allowed_tools_unmatched="warn"),
            new_state(), _ctx(),
        )

    assert out["text"] == "done"
    assert any("matched zero discovered tools" in r.message for r in caplog.records)
