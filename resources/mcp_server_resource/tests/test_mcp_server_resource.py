"""MCPServerResourceComponent: registers an MCPServerResource with the fields other MCP-aware
components (mcp_tool_call/mcp_tool_picker) already use for their own inline `server:` spec, so
a consuming component can build the exact same config dict regardless of which path it came
from.
"""
import dagster as dg


def test_build_defs_registers_the_resource_under_resource_key(mod):
    component = mod.MCPServerResourceComponent(
        resource_key="dagster_plus_mcp", name="dgp", type="http",
        url="https://mcp.agent.dagster.cloud/mcp/",
        headers={"Dagster-Cloud-Organization": "my-org"},
        headers_env={"Authorization": "DAGSTER_PLUS_BEARER"},
    )
    defs = component.build_defs(context=None)
    resource = defs.resources["dagster_plus_mcp"]
    assert isinstance(resource, mod.MCPServerResource)
    assert resource.name == "dgp"
    assert resource.type == "http"
    assert resource.url == "https://mcp.agent.dagster.cloud/mcp/"
    assert resource.headers == {"Dagster-Cloud-Organization": "my-org"}
    assert resource.headers_env == {"Authorization": "DAGSTER_PLUS_BEARER"}


def test_default_resource_key(mod):
    component = mod.MCPServerResourceComponent(name="stdio-server", command=["my-mcp-server"])
    defs = component.build_defs(context=None)
    assert "mcp_server" in defs.resources


def test_resource_dumps_the_same_shape_a_consumer_expects(mod):
    """A consuming component builds server_cfg from an inline MCPServerSpec via
    `.model_dump()` -- MCPServerResource must dump to the exact same key set."""
    resource = mod.MCPServerResource(name="s", type="stdio", command=["x"])
    dumped = resource.model_dump()
    assert set(dumped) >= {"name", "type", "command", "url", "env", "headers", "headers_env"}
