"""Real gap reported from production (Hooli feedback): when the provider rejected the very
first request (OpenAI `insufficient_quota`), the agent step failed with an opaque
`ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)` -- anyio's TaskGroup
wraps whatever exception propagates out of `_run_agent`'s `async with AsyncExitStack()`
block (which owns the MCP stdio client) during the client's teardown, burying the real
provider error. An hour was lost reading it as an MCP problem.

Fixed with `_unwrap_exception_group` (duck-typed on `.exceptions`, so no import of either
Python 3.11's builtin or the 3.10 `exceptiongroup` backport) and `_provider_error_metadata`,
used at the `asyncio.run(_run_agent(...))` call site in the asset body to turn the leaf
error into a clean `dg.Failure` with provider/status/message metadata.
"""
import asyncio
import json
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module
from .test_mcp_real_server import stdio_cfg, server_py  # noqa: F401

mod = load_component_module()


# ── unit tests for the two helpers (synthetic nesting, no real asyncio/MCP needed) ──────────

class _FakeGroup(Exception):
    def __init__(self, message, exceptions):
        super().__init__(message)
        self.exceptions = exceptions


class _FakeCancelled(Exception):
    pass


class _FakeProviderError(Exception):
    def __init__(self, message, llm_provider, status_code):
        super().__init__(message)
        self.message = message
        self.llm_provider = llm_provider
        self.status_code = status_code


def test_unwrap_exception_group_finds_the_leaf_through_nesting():
    leaf = _FakeProviderError("You have no credits remaining.", "openai", 429)
    group = _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [
        _FakeGroup("nested", [leaf]),
    ])
    assert mod._unwrap_exception_group(group) is leaf


def test_unwrap_exception_group_skips_cancellation_noise():
    leaf = _FakeProviderError("boom", "anthropic", 500)
    group = _FakeGroup("unhandled errors in a TaskGroup (2 sub-exceptions)", [_FakeCancelled(), leaf])
    assert mod._unwrap_exception_group(group) is leaf


def test_unwrap_exception_group_is_a_noop_on_a_plain_exception():
    leaf = ValueError("not a group at all")
    assert mod._unwrap_exception_group(leaf) is leaf


def test_provider_error_metadata_extracts_provider_status_message():
    leaf = _FakeProviderError("You have no credits remaining.", "openai", 429)
    info = mod._provider_error_metadata(leaf)
    assert info == {
        "error_class": "_FakeProviderError",
        "provider": "openai",
        "status_code": 429,
        "error_message": "You have no credits remaining.",
    }


def test_provider_error_metadata_falls_back_to_str_when_no_known_attrs():
    info = mod._provider_error_metadata(RuntimeError("plain failure"))
    assert info["provider"] is None
    assert info["status_code"] is None
    assert info["error_message"] == "plain failure"


# ── end-to-end: confirm the real hazard, then confirm the asset body cleans it up ───────────

def test_run_agent_called_directly_really_does_wrap_a_provider_error_in_a_group(stdio_cfg, monkeypatch):
    """Documents the actual hazard: calling `_run_agent` directly (bypassing the asset body's
    new except block) over a REAL stdio MCP connection, with a provider error on the very
    first model call, DOES come back wrapped in a BaseExceptionGroup -- this is anyio's
    TaskGroup teardown behavior, not something this component does on purpose."""

    async def acompletion(messages, **kw):
        raise _FakeProviderError("You have no credits remaining.", "openai", 429)

    fake = types.ModuleType("litellm")
    fake.acompletion = acompletion
    monkeypatch.setitem(sys.modules, "litellm", fake)

    with pytest.raises(BaseException) as excinfo:
        asyncio.run(asyncio.wait_for(
            mod._run_agent(
                log=types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None),
                prompt="go", system_prompt=None, model="m", api_key_env_var=None, api_base_env_var=None,
                temperature=0.0, max_tokens=64, max_iterations=3, mcp_servers=[stdio_cfg],
            ),
            30,
        ))
    leaf = mod._unwrap_exception_group(excinfo.value)
    assert isinstance(leaf, _FakeProviderError)
    assert leaf.llm_provider == "openai" and leaf.status_code == 429


def test_asset_body_unwraps_and_raises_a_clean_failure_with_provider_metadata(monkeypatch):
    """The actual fix, exercised through the real asset body via dg.materialize: a provider
    error -- wrapped in a BaseExceptionGroup the same way the real stdio teardown would wrap
    it -- surfaces as a clean dg.Failure naming the provider/status/message, not an opaque
    ExceptionGroup."""
    leaf = _FakeProviderError("You have no credits remaining.", "openai", 429)

    async def fake_run_agent(**kwargs):
        raise _FakeGroup("unhandled errors in a TaskGroup (1 sub-exception)", [leaf])

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets

    with pytest.raises(dg.Failure) as excinfo:
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    failure = excinfo.value
    assert "openai" in failure.description and "429" in failure.description
    assert "You have no credits remaining." in failure.description
    md = {k: v.value for k, v in (failure.metadata or {}).items()}
    assert md["provider"] == "openai"
    assert md["status_code"] == "429"
    assert md["error_message"] == "You have no credits remaining."


def test_asset_body_does_not_alter_a_plain_ungrouped_exception(monkeypatch):
    """When the error ISN'T wrapped in a group (the common case for errors outside the MCP
    stdio teardown window), the original exception propagates unchanged -- no unnecessary
    wrapping."""

    async def fake_run_agent(**kwargs):
        raise ValueError("some other bug, nothing to do with providers or exception groups")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets

    with pytest.raises(ValueError, match="some other bug"):
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
