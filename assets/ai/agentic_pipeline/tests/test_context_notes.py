"""`context_notes` (pipeline-level): free text naming what's deliberate in the repo/environment
a pipeline reviews -- appended to the system prompt of every LLM-calling step under its own
heading (same block `litellm_agent.context_notes` already uses), and surfaced on each emitted
step's own asset metadata. Set via the reserved `state["__context_notes__"]` key (what
`build_defs` threads in from `self.context_notes`) and activated for the duration of one step
by `_run_step` -- so these tests exercise it the same way `_run_step` does, not by calling an
op function directly (which would see the contextvar unset)."""
import logging
from types import SimpleNamespace

from .conftest import new_state, tool_call

NOTES = "The flaky_job asset is deliberately flaky for this demo; do not file a bug for it."


def _ctx():
    return SimpleNamespace(log=logging.getLogger("agentic_test"))


def _state_with_notes():
    state = new_state()
    state["__context_notes__"] = NOTES
    return state


def test_llm_call_system_prompt_gets_context_notes(mod, fake_litellm):
    fake_litellm.reply("ok")
    state = _state_with_notes()
    mod._run_step({"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "system_prompt": "Be concise."}, state, _ctx())

    sent_system = fake_litellm.calls[0]["messages"][0]["content"]
    assert "Be concise." in sent_system
    assert "## What's deliberate here (not a problem)" in sent_system
    assert NOTES in sent_system


def test_llm_call_metadata_surfaces_context_notes(mod, fake_litellm):
    fake_litellm.reply("ok")
    state = _state_with_notes()
    mod._run_step({"id": "a", "op": "llm_call", "model": "gpt-4o-mini"}, state, _ctx())

    assert state["a"]["context_notes"] == NOTES


def test_no_context_notes_means_no_block(mod, fake_litellm):
    fake_litellm.reply("ok")
    state = new_state()  # no __context_notes__ key at all
    mod._run_step({"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "system_prompt": "Be concise."}, state, _ctx())

    sent_system = fake_litellm.calls[0]["messages"][0]["content"]
    assert sent_system == "Be concise."
    assert "context_notes" not in state["a"]


def test_tool_use_loop_system_prompt_gets_context_notes(mod, fake_litellm, monkeypatch):
    async def discover(log, servers):
        return []

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f1")])

    state = _state_with_notes()
    step = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
            "mcp_servers": [{"name": "svc", "type": "stdio", "command": ["x"]}]}
    mod._run_step(step, state, _ctx())

    sent_system = fake_litellm.calls[0]["messages"][0]["content"]
    assert "## What's deliberate here (not a problem)" in sent_system
    assert NOTES in sent_system
    assert state["agent"]["context_notes"] == NOTES


def test_mcp_call_has_no_context_notes_metadata(mod, monkeypatch):
    """mcp_call never calls an LLM, so there's no system prompt for context_notes to reach --
    but the generic per-step surfacing in _run_step still attaches it to the result/metadata."""
    async def call(**kw):
        return {"raw": "{}", "value": {}, "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    state = _state_with_notes()
    step = {"id": "a", "op": "mcp_call", "server": {"name": "svc", "type": "stdio", "command": ["x"]}, "mcp_tool_name": "t"}
    mod._run_step(step, state, _ctx())

    # no LLM call happened at all (nothing to assert about a system prompt) -- but the pipeline-level
    # note is still visible on this step's own metadata, same as any other step.
    assert state["a"]["context_notes"] == NOTES
