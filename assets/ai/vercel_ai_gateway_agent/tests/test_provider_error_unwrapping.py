"""Real gap reported from production (Hooli feedback): when the provider rejected the very
first request, the agent step failed with an opaque `ExceptionGroup: unhandled errors in a
TaskGroup (1 sub-exception)` -- anyio's TaskGroup wraps whatever exception propagates out of
`_run_agent`'s `async with AsyncExitStack()` block (which owns the MCP stdio client) during
the client's teardown, burying the real provider error. An hour was lost reading it as an
MCP problem.

This component additionally wraps each model attempt in a fallback try/except (`except
Exception as e: ... continue`), so the fix here has a second job beyond the other 5 agent
components: unwrap INSIDE that except block too, so (a) the warning log and the
insufficient_funds string-match both see the clean leaf error instead of an opaque group,
and (b) falling back to the next model on a real provider error still works. See
litellm_agent/tests/test_provider_error_unwrapping.py for the real-stdio reproduction of the
underlying anyio hazard (provider-agnostic, proven there once).
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _FakeGroup(Exception):
    def __init__(self, message, exceptions):
        super().__init__(message)
        self.exceptions = exceptions


class _FakeCancelled(Exception):
    pass


class _FakeProviderError(Exception):
    def __init__(self, message, provider, status_code):
        super().__init__(message)
        self.message = message
        self.provider = provider
        self.status_code = status_code


def test_unwrap_exception_group_finds_the_leaf_through_nesting():
    leaf = _FakeProviderError("rate limited", "openai", 429)
    group = _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [_FakeGroup("nested", [leaf])])
    assert mod._unwrap_exception_group(group) is leaf


def test_unwrap_exception_group_is_a_noop_on_a_plain_exception():
    leaf = ValueError("not a group at all")
    assert mod._unwrap_exception_group(leaf) is leaf


def test_provider_error_metadata_extracts_provider_status_message():
    leaf = _FakeProviderError("rate limited", "openai", 429)
    info = mod._provider_error_metadata(leaf)
    assert info == {
        "error_class": "_FakeProviderError",
        "provider": "openai",
        "status_code": 429,
        "error_message": "rate limited",
    }


def test_a_grouped_error_on_the_first_model_still_falls_back_to_the_next(monkeypatch):
    """The exact behavior this fix must NOT break: a provider error on model A (wrapped in a
    group, same as the real stdio-teardown hazard would produce) should still fall through to
    model B, which succeeds."""

    async def fake_run_agent(**kwargs):
        if kwargs["model"] == "openai/gpt-4o-mini":
            raise _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [
                _FakeProviderError("rate limited", "openai", 429),
            ])
        return {
            "final_answer": "done", "iterations": 1, "tool_calls_made": 0, "tool_call_details": [],
            "transcript": [], "stopped_reason": "final_answer", "mcp_servers": [],
        }

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi",
        model="openai/gpt-4o-mini", fallback_models=["anthropic/claude-sonnet-4-6"],
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    output = result.output_for_node("agent_out")
    assert output["model_used"] == "anthropic/claude-sonnet-4-6"


def test_all_models_exhausted_raises_a_clean_failure_not_a_raw_group(monkeypatch):
    leaf = _FakeProviderError("rate limited", "anthropic", 429)

    async def fake_run_agent(**kwargs):
        raise _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [leaf])

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi",
        model="openai/gpt-4o-mini", fallback_models=["anthropic/claude-sonnet-4-6"],
    )
    assets = component.build_defs(None).assets

    with pytest.raises(dg.Failure) as excinfo:
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    failure = excinfo.value
    assert "rate limited" in failure.description
    md = {k: v.value for k, v in (failure.metadata or {}).items()}
    assert md["provider"] == "anthropic"
    assert md["status_code"] == "429"
