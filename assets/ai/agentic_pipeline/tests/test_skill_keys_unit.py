"""Mocked-unit coverage for `skill_keys` complementing test_skill_keys.py's real
end-to-end test (same split as test_server_resource_key.py / test_mcp_server_resource_key.py
for MCP's own server_resource_key): resource-resolution error paths, `_collect_skill_resource_keys`
recursion through `sub_pipeline`, and `read_skill_file`'s tool-dispatch wrapping an escaping-path
error as a tool result instead of crashing the loop. Only the skill resource object itself is
mocked here (a bare object exposing describe()/load()/read_file()) -- nothing about the LLM
loop or MCP plumbing is faked beyond this suite's existing `fake_litellm` pattern.
"""
import logging
from types import SimpleNamespace

import pytest

from .conftest import tool_call


def _ctx_with_resources(**resources):
    return SimpleNamespace(log=logging.getLogger("agentic_test"), resources=SimpleNamespace(**resources))


class _FakeSkillResource:
    def __init__(self, name="fake-skill", description="A fake skill.", content="fake skill body", read_file_error=None):
        self._name = name
        self._description = description
        self._content = content
        self._read_file_error = read_file_error

    def describe(self):
        return {"key": "fake_skill", "name": self._name, "description": self._description}

    def load(self):
        return {"content": self._content, "files": []}

    def read_file(self, relative_path):
        if self._read_file_error is not None:
            raise self._read_file_error
        return f"contents of {relative_path}"


# ── skill_keys resolution errors ───────────────────────────────────────────────

def test_skill_keys_unresolved_resource_key_raises_naming_the_key(mod):
    from .conftest import new_state

    ctx = _ctx_with_resources()  # no `missing_key` registered
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["missing_key"],
    }
    with pytest.raises(ValueError, match="missing_key"):
        mod._do_tool_use_loop(step, new_state(), ctx)


def test_skill_keys_resource_not_a_skill_resource_raises(mod):
    from .conftest import new_state

    ctx = _ctx_with_resources(not_a_skill=object())
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["not_a_skill"],
    }
    with pytest.raises(ValueError, match="not_a_skill"):
        mod._do_tool_use_loop(step, new_state(), ctx)


# ── _collect_skill_resource_keys ────────────────────────────────────────────────

def test_collect_skill_resource_keys_unions_tool_use_loop_steps(mod):
    steps = [
        {"id": "a", "op": "tool_use_loop", "mcp_servers": [], "skill_keys": ["k1", "k2"]},
        {"id": "b", "op": "tool_use_loop", "mcp_servers": [], "skill_keys": ["k2", "k3"]},
        {"id": "c", "op": "mcp_call", "server": {"name": "x"}},  # mcp_call never carries skill_keys
    ]
    assert mod._collect_skill_resource_keys(steps) == {"k1", "k2", "k3"}


def test_collect_skill_resource_keys_recurses_into_sub_pipeline(mod):
    steps = [
        {"id": "outer", "op": "sub_pipeline", "steps": [
            {"id": "inner", "op": "tool_use_loop", "mcp_servers": [], "skill_keys": ["nested_key"]},
            {"id": "inner2", "op": "sub_pipeline", "steps": [
                {"id": "deepest", "op": "tool_use_loop", "mcp_servers": [], "skill_keys": ["deep_key"]},
            ]},
        ]},
    ]
    assert mod._collect_skill_resource_keys(steps) == {"nested_key", "deep_key"}


def test_collect_skill_resource_keys_empty_when_unconfigured(mod):
    steps = [{"id": "a", "op": "tool_use_loop", "mcp_servers": []}]
    assert mod._collect_skill_resource_keys(steps) == set()


# ── read_skill_file tool dispatch ───────────────────────────────────────────────

@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    async def discover(log, servers):
        return [{"server": s.get("name") or "server", "name": "echo", "description": "",
                  "input_schema": {"type": "object", "properties": {}}} for s in servers]

    async def call(**kw):
        return {"raw": "ok", "kind": "text"}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)


def test_read_skill_file_escaping_path_is_a_tool_error_not_a_crash(mod, fake_litellm, tool_use_loop_capture):
    from .conftest import new_state

    fake_litellm.reply("", tool_calls=[tool_call(
        "read_skill_file", {"name": "fake-skill", "relative_path": "../../etc/passwd"}, id="t1",
    )])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    fake_resource = _FakeSkillResource(read_file_error=ValueError("'../../etc/passwd' escapes the skill folder /tmp/fake"))
    ctx = _ctx_with_resources(fake_skill=fake_resource)
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["fake_skill"],
    }

    out = mod._do_tool_use_loop(step, new_state(), ctx)

    # Loop did not crash -- it kept going and finalized normally.
    assert out["text"] == "done"
    error_entries = [t for t in out["tool_call_trace"] if t.get("phase") == "tool_call_error"]
    assert len(error_entries) == 1
    assert "escapes the skill folder" in error_entries[0]["error"]
    # read_skill_file failures must not bump skill_load_count (only load_skill does).
    assert out["skill_load_count"] == 0
    assert out["skills_loaded"] == []


def test_read_skill_file_unknown_skill_name_is_a_tool_error_not_a_crash(mod, fake_litellm, tool_use_loop_capture):
    from .conftest import new_state

    fake_litellm.reply("", tool_calls=[tool_call(
        "read_skill_file", {"name": "does-not-exist", "relative_path": "a.txt"}, id="t1",
    )])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    ctx = _ctx_with_resources(fake_skill=_FakeSkillResource())
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["fake_skill"],
    }

    out = mod._do_tool_use_loop(step, new_state(), ctx)
    assert out["text"] == "done"
    error_entries = [t for t in out["tool_call_trace"] if t.get("phase") == "tool_call_error"]
    assert len(error_entries) == 1
    assert "unknown skill" in error_entries[0]["error"]


def test_load_skill_then_read_skill_file_both_resolve_against_same_fake_resource(mod, fake_litellm, tool_use_loop_capture):
    from .conftest import new_state

    fake_litellm.reply("", tool_calls=[tool_call("load_skill", {"name": "fake-skill"}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("read_skill_file", {"name": "fake-skill", "relative_path": "template.txt"}, id="t2")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    ctx = _ctx_with_resources(fake_skill=_FakeSkillResource())
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["fake_skill"],
    }

    out = mod._do_tool_use_loop(step, new_state(), ctx)
    assert out["text"] == "done"
    assert out["skill_load_count"] == 1  # read_skill_file does not increment it
    assert out["skills_loaded"] == ["fake-skill"]
