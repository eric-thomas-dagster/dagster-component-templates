"""Real gap reported from production (Hooli feedback): when the provider rejected the very
first request (e.g. OpenAI `insufficient_quota`), the agent step failed with an opaque
`ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)` -- anyio's TaskGroup
wraps whatever exception propagates out of `_run_agent`'s `async with AsyncExitStack()`
block (which owns the MCP stdio client) during the client's teardown, burying the real
provider error. An hour was lost reading it as an MCP problem.

Fixed with `_unwrap_exception_group` (duck-typed on `.exceptions`, so no import of either
Python 3.11's builtin or the 3.10 `exceptiongroup` backport) and `_provider_error_metadata`,
used at the `asyncio.run(_run_agent(...))` call site in the asset body to turn the leaf
error into a clean `dg.Failure` with provider/status/message metadata. See
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
    def __init__(self, message, status_code):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def test_unwrap_exception_group_finds_the_leaf_through_nesting():
    leaf = _FakeProviderError("You have no credits remaining.", 429)
    group = _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [_FakeGroup("nested", [leaf])])
    assert mod._unwrap_exception_group(group) is leaf


def test_unwrap_exception_group_skips_cancellation_noise():
    leaf = _FakeProviderError("boom", 500)
    group = _FakeGroup("unhandled errors in a TaskGroup (2 sub-exceptions)", [_FakeCancelled(), leaf])
    assert mod._unwrap_exception_group(group) is leaf


def test_unwrap_exception_group_is_a_noop_on_a_plain_exception():
    leaf = ValueError("not a group at all")
    assert mod._unwrap_exception_group(leaf) is leaf


def test_provider_error_metadata_extracts_status_and_message():
    leaf = _FakeProviderError("You have no credits remaining.", 429)
    info = mod._provider_error_metadata(leaf)
    assert info["status_code"] == 429
    assert info["error_message"] == "You have no credits remaining."
    assert info["error_class"] == "_FakeProviderError"


def test_asset_body_unwraps_and_raises_a_clean_failure_with_provider_metadata(monkeypatch):
    leaf = _FakeProviderError("You have no credits remaining.", 429)

    async def fake_run_agent(**kwargs):
        raise _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [leaf])

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.OpenAIAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets

    with pytest.raises(dg.Failure) as excinfo:
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    failure = excinfo.value
    assert "429" in failure.description
    assert "You have no credits remaining." in failure.description
    md = {k: v.value for k, v in (failure.metadata or {}).items()}
    assert md["status_code"] == "429"
    assert md["error_message"] == "You have no credits remaining."


def test_asset_body_does_not_alter_a_plain_ungrouped_exception(monkeypatch):
    async def fake_run_agent(**kwargs):
        raise ValueError("some other bug, nothing to do with providers or exception groups")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.OpenAIAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets

    with pytest.raises(ValueError, match="some other bug"):
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
