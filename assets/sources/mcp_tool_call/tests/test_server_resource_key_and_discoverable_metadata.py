"""server_resource_key lets several components share one registered MCPServerResource
instead of each repeating the same inline server spec, and the new discoverable
tags/metadata (dagster-community/mcp-server, dagster-community/mcp-tool, mcp_tool,
transport, latency_seconds, response_bytes, parsed_as, response_preview) let a UI find
and group MCP tool-call assets without parsing existing keys. Existing keys/tags are
unchanged -- these are purely additive.
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

    async def fake_call_tool(*, log, server_cfg, tool_name, tool_args, parse_as):
        seen.append(server_cfg)
        raw = '{"ok": true}'
        return {"is_error": False, "kind": "json", "value": {"ok": True}, "raw": raw, "raw_size": len(raw)}

    monkeypatch.setattr(mod, "_call_tool", fake_call_tool)
    return seen


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


def test_requires_exactly_one_of_server_and_server_resource_key(mod):
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.MCPToolCallComponent(asset_name="a", tool_name="t")
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.MCPToolCallComponent(
            asset_name="a", tool_name="t",
            server={"name": "s", "type": "http", "url": "http://x"},
            server_resource_key="shared",
        )


def test_server_resource_key_resolves_the_resource_at_runtime(mod, calls):
    from resources.mcp_server_resource.component import MCPServerResource

    component = mod.MCPToolCallComponent(
        asset_name="mcp_out", server_resource_key="shared_mcp", tool_name="t", tool_args={},
    )
    resource = MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp")
    result = _run(component, resources={"shared_mcp": resource})
    assert result.success
    assert calls[0]["name"] == "dgp"
    assert calls[0]["url"] == "http://localhost:1/mcp"


def test_tag_uses_resource_key_when_shared_server_name_is_not_known_at_build_defs_time(mod):
    component = mod.MCPToolCallComponent(asset_name="mcp_out", server_resource_key="shared_mcp", tool_name="t")
    asset_def = component.build_defs(None).assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/mcp-server"] == "shared_mcp"
    assert tags["dagster-community/mcp-tool"] == "t"


def test_tag_uses_inline_server_name(mod):
    component = mod.MCPToolCallComponent(
        asset_name="mcp_out", server={"name": "inline-srv", "type": "http", "url": "http://x"}, tool_name="t",
    )
    asset_def = component.build_defs(None).assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/mcp-server"] == "inline-srv"


def test_new_discoverable_metadata_present_alongside_existing_keys(mod, calls):
    component = mod.MCPToolCallComponent(
        asset_name="mcp_out", server={"name": "inline-srv", "type": "http", "url": "http://x"},
        tool_name="my_tool", tool_args={},
    )
    result = _run(component)
    md = result.get_asset_materialization_events()[-1].materialization.metadata

    # existing keys, unchanged
    assert md["mcp_server"].value == "inline-srv"
    assert md["tool_name"].value == "my_tool"
    assert md["is_error"].value is False
    assert md["result_kind"].value == "json"
    assert md["result_size_bytes"].value >= 0

    # new, additive keys
    assert md["mcp_tool"].value == "my_tool"
    assert md["transport"].value == "http"
    assert md["latency_seconds"].value >= 0
    assert isinstance(md["latency_seconds"].value, float)
    assert md["response_bytes"].value == md["result_size_bytes"].value
    assert md["parsed_as"].value == "json"
    assert md["response_preview"].data == {"ok": True}
