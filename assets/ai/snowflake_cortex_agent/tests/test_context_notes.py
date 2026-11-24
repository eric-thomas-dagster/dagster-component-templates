"""Real gap reported from production (Hooli feedback): an improvement-advisor agent filed
"disable the artificial flakiness" against a demo repository where the flakiness is the point.
`context_notes` is the place to say what's deliberate (demos, test fixtures, expected failures)
so the model doesn't flag it. Composed into the resolved system prompt in the asset body, before
`_run_agent` is even called (no MCP connection needed to prove this, unlike required_tool_calls
or the ExceptionGroup-unwrap fix), so these tests fake `_run_agent` directly and inspect the
`system_prompt` it was called with.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


def _fake_run_agent_capturing(seen):
    async def fake_run_agent(**kwargs):
        seen["system_prompt"] = kwargs.get("system_prompt")
        return {
            "final_answer": "done", "iterations": 1, "tool_calls_made": 0, "tool_call_details": [],
            "transcript": [], "stopped_reason": "final_answer", "model": "m", "mcp_servers": [],
            "combined_server_instructions": None,
        }
    return fake_run_agent


def test_context_notes_appended_to_the_system_prompt(monkeypatch):
    seen = {}
    monkeypatch.setattr(mod, "_run_agent", _fake_run_agent_capturing(seen))
    component = mod.SnowflakeCortexAgentComponent(
        asset_name="agent_out", prompt="hi", system_prompt="You are a helpful reviewer.",
        context_notes="This is a demo repository; the artificial flakiness in retry_handler.py is deliberate.",
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert "You are a helpful reviewer." in seen["system_prompt"]
    assert "What's deliberate here (not a problem)" in seen["system_prompt"]
    assert "the artificial flakiness in retry_handler.py is deliberate" in seen["system_prompt"]


def test_context_notes_works_with_no_system_prompt_set(monkeypatch):
    seen = {}
    monkeypatch.setattr(mod, "_run_agent", _fake_run_agent_capturing(seen))
    component = mod.SnowflakeCortexAgentComponent(
        asset_name="agent_out", prompt="hi",
        context_notes="Expected test fixture failures: test_flaky_retry is supposed to fail twice before passing.",
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert "Expected test fixture failures" in seen["system_prompt"]


def test_unset_context_notes_adds_nothing(monkeypatch):
    seen = {}
    monkeypatch.setattr(mod, "_run_agent", _fake_run_agent_capturing(seen))
    component = mod.SnowflakeCortexAgentComponent(asset_name="agent_out", prompt="hi", system_prompt="Plain system prompt.")
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert seen["system_prompt"] == "Plain system prompt."
