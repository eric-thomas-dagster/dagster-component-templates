"""server_resource_key on `mcp_call`'s `server` and `tool_use_loop`'s `mcp_servers` entries:
lets a step share a registered MCPServerResource instead of repeating the same inline
name/type/command/url/env/headers/headers_env spec -- same pattern as mcp_tool_call's own
server_resource_key. Steps are raw dicts here (not Pydantic), so the exactly-one-of check
and resolution both live in `_resolve_mcp_server_cfg`, called from `_do_mcp_call` and
`_do_tool_use_loop`. Only the low-level MCP call (`_call_mcp_tool_async` / discovery) is
faked, matching this test suite's existing convention (see test_policy.py's `mcp` fixture).
"""
import logging
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import new_state, tool_call


def _ctx_with_resources(**resources):
    return SimpleNamespace(log=logging.getLogger("agentic_test"), resources=SimpleNamespace(**resources))


@pytest.fixture
def mcp_call_capture(mod, monkeypatch):
    seen = []

    async def call(**kw):
        seen.append(kw["server_cfg"])
        return {"raw": "{}", "value": {}, "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return seen


@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    seen = []

    async def discover(log, servers):
        seen.append(list(servers))
        return [{"server": s.get("name") or "server", "name": "echo", "description": "", "input_schema": {"type": "object", "properties": {}}}
                for s in servers]

    async def call(**kw):
        return {"raw": "ok", "kind": "text"}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return seen


# ── mcp_call ──────────────────────────────────────────────────────────────────

def test_mcp_call_requires_exactly_one_of_inline_and_resource_key(mod):
    ctx = _ctx_with_resources()
    with pytest.raises(ValueError, match="exactly one"):
        mod._do_mcp_call(
            {"id": "a", "op": "mcp_call", "mcp_tool_name": "t",
             "server": {"name": "inline", "type": "http", "url": "http://x", "server_resource_key": "shared_mcp"}},
            new_state(), ctx,
        )


def test_mcp_call_resolves_server_resource_key_at_runtime(mod, mcp_call_capture):
    from resources.mcp_server_resource.component import MCPServerResource

    ctx = _ctx_with_resources(shared_mcp=MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp"))
    out = mod._do_mcp_call(
        {"id": "a", "op": "mcp_call", "mcp_tool_name": "t", "server": {"server_resource_key": "shared_mcp"}},
        new_state(), ctx,
    )
    assert out["server_name"] == "dgp"
    assert mcp_call_capture[0]["name"] == "dgp"
    assert mcp_call_capture[0]["url"] == "http://localhost:1/mcp"


def test_mcp_call_inline_still_works(mod, mcp_call_capture):
    ctx = _ctx_with_resources()
    out = mod._do_mcp_call(
        {"id": "a", "op": "mcp_call", "mcp_tool_name": "t",
         "server": {"name": "inline-srv", "type": "http", "url": "http://x"}},
        new_state(), ctx,
    )
    assert out["server_name"] == "inline-srv"
    assert mcp_call_capture[0]["name"] == "inline-srv"


# ── tool_use_loop ─────────────────────────────────────────────────────────────

def test_tool_use_loop_requires_exactly_one_of_inline_and_resource_key(mod, fake_litellm):
    ctx = _ctx_with_resources()
    with pytest.raises(ValueError, match="exactly one"):
        mod._do_tool_use_loop(
            {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
             "mcp_servers": [{}]},
            new_state(), ctx,
        )


def test_tool_use_loop_resolves_server_resource_key_at_runtime(mod, fake_litellm, tool_use_loop_capture):
    from resources.mcp_server_resource.component import MCPServerResource

    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])
    ctx = _ctx_with_resources(shared_mcp=MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp"))
    out = mod._do_tool_use_loop(
        {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
         "mcp_servers": [{"server_resource_key": "shared_mcp"}]},
        new_state(), ctx,
    )
    assert out["text"] == "done"
    resolved = tool_use_loop_capture[0]
    assert resolved[0]["name"] == "dgp"
    assert resolved[0]["url"] == "http://localhost:1/mcp"


def test_tool_use_loop_inline_still_works(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])
    ctx = _ctx_with_resources()
    out = mod._do_tool_use_loop(
        {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
         "mcp_servers": [{"name": "inline-srv", "type": "stdio", "command": ["x"]}]},
        new_state(), ctx,
    )
    assert out["text"] == "done"
    assert tool_use_loop_capture[0][0]["name"] == "inline-srv"


# ── static required_resource_keys collection ──────────────────────────────────

def test_collect_mcp_server_resource_keys_unions_mcp_call_and_tool_use_loop(mod):
    steps = [
        {"id": "a", "op": "mcp_call", "server": {"server_resource_key": "k1"}},
        {"id": "b", "op": "tool_use_loop", "mcp_servers": [
            {"name": "inline", "type": "stdio", "command": ["x"]},
            {"server_resource_key": "k2"},
        ]},
    ]
    assert mod._collect_mcp_server_resource_keys(steps) == {"k1", "k2"}


def test_collect_mcp_server_resource_keys_recurses_into_sub_pipeline(mod):
    steps = [
        {"id": "outer", "op": "sub_pipeline", "steps": [
            {"id": "inner", "op": "mcp_call", "server": {"server_resource_key": "nested_key"}},
        ]},
    ]
    assert mod._collect_mcp_server_resource_keys(steps) == {"nested_key"}


def test_collect_mcp_server_resource_keys_empty_for_inline_only(mod):
    steps = [{"id": "a", "op": "mcp_call", "server": {"name": "inline", "type": "stdio", "command": ["x"]}}]
    assert mod._collect_mcp_server_resource_keys(steps) == set()


# ── end-to-end: required_resource_keys actually wired on the built asset ──────

def test_pipeline_asset_requires_mcp_resource_key_and_materializes(mod, fake_litellm, mcp_call_capture):
    from resources.mcp_server_resource.component import MCPServerResource

    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": "doc"},
        steps=[{"id": "a", "op": "mcp_call", "mcp_tool_name": "t", "server": {"server_resource_key": "shared_mcp"}}],
        outputs={"assets": ["a"]},
    )
    asset_def = component.build_defs(None).assets[0]
    assert "shared_mcp" in asset_def.required_resource_keys

    resource = MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp")
    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager(), "shared_mcp": resource},
        raise_on_error=True,
    )
    assert result.success
    assert mcp_call_capture[0]["name"] == "dgp"
