"""inspection_asset_name: a real stdio MCP server (written to tmp_path, run as a subprocess) is listed through a
real materialization -- not mocked -- confirming the asset reports the server's REAL tool names + descriptions."""
import sys

import dagster as dg
import pytest

pytest.importorskip("mcp")

SERVER_SRC = """
import sys
try:
    from mcp.server.mcpserver import MCPServer as Server  # mcp 2.x
except ImportError:
    from mcp.server.fastmcp import FastMCP as Server  # mcp 1.x

server = Server("mini")


@server.tool()
def echo(text: str) -> dict:
    "Echo text back."
    return {"echo": text}


@server.tool()
def boom(reason: str) -> dict:
    "Always fails."
    raise ValueError(f"boom: {reason}")


if __name__ == "__main__":
    server.run(transport="stdio")
"""


@pytest.fixture
def server_py(tmp_path):
    p = tmp_path / "mini_mcp_server.py"
    p.write_text(SERVER_SRC)
    return str(p)


def test_inspection_asset_lists_real_tool_names_and_descriptions(mod, server_py):
    component = mod.MCPServerResourceComponent(
        resource_key="srv", name="mini", type="stdio", command=[sys.executable, server_py],
        inspection_asset_name="mini_inspection",
    )
    defs = component.build_defs(context=None)
    assert set(defs.resources) == {"srv"}
    assert {a.key for a in defs.assets} == {dg.AssetKey("mini_inspection")}

    result = dg.materialize(defs.assets, resources=defs.resources)
    assert result.success
    md = {k: v.value for k, v in result.get_asset_materialization_events()[0].materialization.metadata.items()}
    assert md["server_name"] == "mini"
    assert md["tool_count"] == 2
    assert set(md["tool_names"]) == {"echo", "boom"}
    assert md["tool_descriptions"]["echo"] == "Echo text back."
    assert md["tool_descriptions"]["boom"] == "Always fails."


def test_no_inspection_asset_by_default(mod):
    component = mod.MCPServerResourceComponent(resource_key="srv", name="mini", command=["x"])
    defs = component.build_defs(context=None)
    assert not defs.assets
