"""server_resource_key on a per-tool MCPToolPickerToolSpec: lets a picker's tool share a
registered MCPServerResource instead of repeating the same inline `server:` spec --
same pattern as mcp_tool_call's own server_resource_key, since each tool here can live on
a different server. Only the tool-result asset itself is exercised (a fake upstream
"plan" asset stands in for the real planner, so no LLM call is needed).
"""
import dagster as dg
import pandas as pd
import pydantic
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def calls(mod, monkeypatch):
    seen = []

    async def fake_call_mcp_tool(*, log, server_cfg, tool_name, tool_args, parse_as):
        seen.append(server_cfg)
        return {"is_error": False, "kind": "json", "value": {"ok": True}, "raw": '{"ok": true}'}

    monkeypatch.setattr(mod, "_call_mcp_tool", fake_call_mcp_tool)
    return seen


def _component(mod, **tool_kw):
    tool = dict(name="echo", description="echoes text", mcp_tool_name="echo")
    tool.update(tool_kw)
    return mod.MCPToolPickerComponent(
        plan_asset_name="plan", synthesis_asset_name="synth", task="t",
        tools=[tool],
    )


def test_requires_exactly_one_of_server_and_server_resource_key(mod):
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        _component(mod)
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        _component(mod, server={"name": "s", "type": "http", "url": "http://x"}, server_resource_key="shared")


def test_server_resource_key_resolves_the_resource_at_runtime(mod, calls):
    from resources.mcp_server_resource.component import MCPServerResource

    component = _component(mod, server_resource_key="shared_mcp")
    defs = component.build_defs(None)
    tool_asset = next(a for a in defs.assets if "echo_result" in {k.to_user_string() for k in a.keys})

    @dg.asset(name="plan")
    def _fake_plan():
        return pd.DataFrame([{"tool": "echo", "args": "{}", "reason": "test"}])

    resource = MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp")
    result = dg.materialize(
        [_fake_plan, tool_asset],
        resources={"io_manager": dg.InMemoryIOManager(), "shared_mcp": resource},
    )
    assert result.success
    assert calls[0]["name"] == "dgp"
    assert calls[0]["url"] == "http://localhost:1/mcp"


def test_inline_server_still_works(mod, calls):
    component = _component(mod, server={"name": "inline-srv", "type": "http", "url": "http://x"})
    defs = component.build_defs(None)
    tool_asset = next(a for a in defs.assets if "echo_result" in {k.to_user_string() for k in a.keys})

    @dg.asset(name="plan")
    def _fake_plan():
        return pd.DataFrame([{"tool": "echo", "args": "{}", "reason": "test"}])

    result = dg.materialize([_fake_plan, tool_asset], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert calls[0]["name"] == "inline-srv"
