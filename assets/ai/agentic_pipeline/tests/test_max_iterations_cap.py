"""What actually happens when `tool_use_loop` hits `max_iterations` without the model ever
calling `finalize` -- Hooli's own live run (15/30 iterations, finalize called cleanly) didn't hit
the cap, so this is answered with a cheap deterministic test (a scripted model that always calls
one more tool, run with an artificially low max_iterations) rather than another expensive real
run: the cap logic itself doesn't care whether tokens are real or scripted. Only the low-level MCP
discover/call is faked, matching test_required_tool_calls.py's convention."""
import logging
from types import SimpleNamespace

import pytest

from .conftest import new_state, tool_call

SERVER = [{"name": "svc", "type": "stdio", "command": ["x"]}]


@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    calls = []

    async def discover(log, servers):
        return [{"server": "svc", "name": "read_file", "description": "", "input_schema": {"type": "object", "properties": {}}}]

    async def call(**kw):
        calls.append(kw["tool_name"])
        return {"raw": f"contents of file {len(calls)}", "value": {"text": f"contents of file {len(calls)}"},
                "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return calls


def _ctx():
    return SimpleNamespace(log=logging.getLogger("agentic_test"))


def test_hitting_the_cap_preserves_full_trajectory_and_does_not_raise(mod, fake_litellm, tool_use_loop_capture):
    """The model never calls finalize -- just keeps reading one more file, forever. With
    max_iterations: 3, the loop must stop cleanly after exactly 3 rounds (no exception), and every
    tool call made along the way must still be in tool_call_trace -- nothing lost or truncated."""
    for i in range(1, 6):  # script more replies than the cap allows, to prove the extras are never reached
        fake_litellm.reply("", tool_calls=[tool_call("svc__read_file", {"path": f"f{i}.txt"}, id=f"t{i}")])

    step = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER, "max_iterations": 3}
    out = mod._do_tool_use_loop(step, new_state(), _ctx())

    assert out["stop_reason"] == "max_iterations_hit"
    assert out["n_iterations"] == 3
    # all 3 (and only 3) tool calls were actually made -- the cap stopped it, not an early exit
    assert tool_use_loop_capture == ["read_file", "read_file", "read_file"]
    # the full trajectory (all 3 tool calls + their results) survives in metadata -- nothing is
    # dropped when the cap is hit, only the final-answer synthesis is skipped
    tool_phases = [t for t in out["tool_call_trace"] if t.get("phase") in ("tool_call", "tool_call_result")]
    assert len(out["tool_call_trace"]) >= 3
    assert all(f"f{i}.txt" in str(out["tool_call_trace"]) for i in (1, 2, 3))
    # NOT a best-effort finalize with partial context -- a fixed placeholder, the model is never
    # asked to synthesize an answer from what it gathered
    assert "terminated after 3 iterations" in out["text"]
    assert "finalize" in out["text"]


def test_finalize_before_the_cap_is_unaffected(mod, fake_litellm, tool_use_loop_capture):
    """Sanity check: a model that finalizes within the cap gets its real answer, not the
    placeholder -- the cap-hit path only triggers when finalize is never reached."""
    fake_litellm.reply("", tool_calls=[tool_call("svc__read_file", {"path": "f1.txt"}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "real synthesized answer"}, id="f1")])

    step = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "mcp_servers": SERVER, "max_iterations": 3}
    out = mod._do_tool_use_loop(step, new_state(), _ctx())

    assert out["stop_reason"] != "max_iterations_hit"
    assert out["text"] == "real synthesized answer"
