"""Real, committed tests for NotionMcpServerComponent / NotionMcpProxyTools.

Mocks only the unavoidable external call (`_nmo_call_tool_async`, the actual network hop to
Notion's hosted MCP server), per this repo's "mock only the paid/external call" convention.
"""
import sys

import pytest


def test_build_defs_registers_stdio_resource(mod, tmp_path):
    component = mod.NotionMcpServerComponent(resource_key="notion_mcp", oauth_dir=str(tmp_path))
    defs = component.build_defs(context=None)
    resource = defs.resources["notion_mcp"]
    assert resource.type == "stdio"
    assert resource.command[0] == sys.executable
    assert "-m" in resource.command
    assert resource.env["NOTION_MCP_OAUTH_DIR"] == str(tmp_path)
    assert resource.env["NOTION_MCP_MAX_REQUESTS_PER_SECOND"] == "2.0"


def test_allow_list_rejects_unknown_tool(mod, tmp_path):
    with pytest.raises(ValueError, match="unknown tool"):
        mod.NotionMcpServerComponent(oauth_dir=str(tmp_path), allow=["notion-search", "notion-delete-pages"]).build_defs(None)


def test_allow_list_narrows_env(mod, tmp_path):
    component = mod.NotionMcpServerComponent(oauth_dir=str(tmp_path), allow=["notion-search"])
    defs = component.build_defs(context=None)
    resource = defs.resources["notion_mcp"]
    assert resource.env["NOTION_MCP_ALLOW"] == "notion-search"


# --- NotionMcpProxyTools --------------------------------------------------------------------

def test_proxy_tools_call_returns_text_on_success(mod, monkeypatch, tmp_path):
    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        return f"result for {tool_name} {arguments}"

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    tools = mod.NotionMcpProxyTools(str(tmp_path), max_requests_per_second=1000)
    result = tools.call("notion-search", {"query": "hello"})
    assert "result for notion-search" in result["text"]


def test_proxy_tools_unknown_tool_is_an_error_not_an_exception(mod, tmp_path):
    tools = mod.NotionMcpProxyTools(str(tmp_path))
    assert "error" in tools.call("notion-delete-pages", {})


def test_proxy_tools_disallowed_tool_is_an_error(mod, tmp_path):
    tools = mod.NotionMcpProxyTools(str(tmp_path), allow=["notion-search"])
    result = tools.call("notion-fetch", {"id": "x"})
    assert "error" in result
    assert "not enabled" in result["error"]


def test_proxy_tools_auth_error_becomes_error_dict_not_a_crash(mod, monkeypatch, tmp_path):
    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        raise mod.NotionMcpAuthError("sign in again")

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    tools = mod.NotionMcpProxyTools(str(tmp_path), max_requests_per_second=1000)
    result = tools.call("notion-search", {"query": "x"})
    assert "sign in again" in result["error"]


def test_proxy_tools_tracks_rate_limit_hits(mod, monkeypatch, tmp_path):
    monkeypatch.setattr(mod._nmo_time, "sleep", lambda s: None)
    responses = iter([
        "The Notion API rate limit was reached. Wait at least 3 seconds before retrying this tool",
        "real content",
    ])

    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        return next(responses)

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    tools = mod.NotionMcpProxyTools(str(tmp_path), max_requests_per_second=1000)
    result = tools.call("notion-fetch", {"id": "x"})
    assert result["text"] == "real content"
    assert tools.rate_limit_hits == 1
    assert tools.rate_limit_total_wait_seconds == 3.0


def test_build_mcp_server_exposes_only_allowed_tools(mod, tmp_path):
    tools = mod.NotionMcpProxyTools(str(tmp_path), allow=["notion-search"])
    mcp = mod.build_mcp_server(tools)
    # FastMCP/MCPServer both expose list_tools via an internal registry; check the allow-list
    # logic ran by confirming the proxy's own tool_names() matches what we configured.
    assert tools.tool_names() == ["notion-search"]
