"""Per-server `instructions` end up as run metadata on the materialized asset (item 8,
point 4: "Show the combined text in the run metadata so people can see what the model was
told."). Drives the real component through dg.materialize(); only the model call is faked."""
import inspect
import sys
import textwrap
import types

import dagster as dg
import pytest

from .conftest import load_component_module

pytest.importorskip("mcp")

SERVER = textwrap.dedent('''
    try:
        from mcp.server.mcpserver import MCPServer as _Server  # mcp 2.x renamed FastMCP
    except ImportError:
        from mcp.server.fastmcp import FastMCP as _Server  # mcp 1.x
    mcp = _Server("tools")

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    mcp.run(transport="stdio")
''')


class _Msg:
    def __init__(self, content):
        self.content, self.tool_calls = content, None

    def model_dump(self, exclude_none=True):
        return {"role": "assistant", "content": self.content}


def _materialize(monkeypatch, tmp_path, mcp_servers):
    script = tmp_path / "server.py"
    script.write_text(SERVER)

    async def acompletion(messages, **kw):
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg("done"))])

    fake = types.ModuleType("litellm")
    fake.acompletion = acompletion
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    for cfg in mcp_servers:
        if cfg.get("type") == "stdio" and "command" not in cfg:
            cfg["command"] = [sys.executable, str(script)]
    comp = cls(asset_name="agent_out", prompt="what is 2+3?", model="m", mcp_servers=mcp_servers)
    res = dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.get_asset_materialization_events()[0].materialization.metadata


def test_saved_instructions_surface_in_run_metadata(monkeypatch, tmp_path):
    md = _materialize(monkeypatch, tmp_path, [{
        "name": "tools", "type": "stdio", "instructions": "Always show amounts in cents.",
    }])
    assert "mcp_server_instructions" in md
    text = md["mcp_server_instructions"].value
    assert "## tools" in text
    assert "Always show amounts in cents." in text


def test_no_saved_or_server_instructions_means_no_metadata_key(monkeypatch, tmp_path):
    md = _materialize(monkeypatch, tmp_path, [{"name": "tools", "type": "stdio"}])
    assert "mcp_server_instructions" not in md
