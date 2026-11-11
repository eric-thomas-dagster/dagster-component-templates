"""server_resource_key on a per-entry MCPServerSpec: lets an mcp_servers[] entry share a
registered MCPServerResource instead of repeating the same inline name/type/command/url/
env/headers/headers_env spec -- same pattern as mcp_tool_call's own server_resource_key.
Only `_run_agent` is faked (the real MCP connect loop); resolving the effective per-entry
config from either path is exercised through a real dg.materialize() run.
"""
import dagster as dg
import pydantic
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def calls(mod, monkeypatch):
    seen = []

    async def fake_run_agent(*, log, prompt, system_prompt, model, api_key_env_var, api_base_url,
                              temperature, max_tokens, max_iterations, mcp_servers, **kwargs):
        seen.append(mcp_servers)
        return {
            "final_answer": "done", "iterations": 1, "tool_calls_made": 0,
            "tool_call_details": [], "transcript": [], "stopped_reason": "final_answer",
            "model": model, "mcp_servers": [s.get("name") for s in mcp_servers],
        }

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    return seen


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


def test_requires_exactly_one_of_name_and_server_resource_key(mod):
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.MCPServerSpec()
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.MCPServerSpec(name="inline-srv", type="http", url="http://x", server_resource_key="shared")


def test_server_resource_key_resolves_the_resource_at_runtime(mod, calls):
    from resources.mcp_server_resource.component import MCPServerResource

    component = mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi",
        mcp_servers=[{"server_resource_key": "shared_mcp"}],
    )
    resource = MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp")
    result = _run(component, resources={"shared_mcp": resource})
    assert result.success
    assert calls[0][0]["name"] == "dgp"
    assert calls[0][0]["url"] == "http://localhost:1/mcp"


def test_inline_server_still_works(mod, calls):
    component = mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi",
        mcp_servers=[{"name": "inline-srv", "type": "http", "url": "http://x"}],
    )
    result = _run(component)
    assert result.success
    assert calls[0][0]["name"] == "inline-srv"


def test_required_resource_keys_only_includes_shared_entries(mod):
    component = mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi",
        mcp_servers=[
            {"name": "inline-srv", "type": "http", "url": "http://x"},
            {"server_resource_key": "shared_mcp"},
        ],
    )
    asset_def = component.build_defs(None).assets[0]
    assert "shared_mcp" in asset_def.required_resource_keys
    assert "inline-srv" not in asset_def.required_resource_keys
