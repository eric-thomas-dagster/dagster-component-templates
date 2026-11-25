"""Full round trip through a real stdio subprocess (same transport agentic_pipeline/slack_chat_tools
etc. use), not just calling the registered tool function in-process -- the SDK's tool dispatch only
preserves a raised exception's own message when it recognizes the exception as ITS OWN ToolError (see
_mcp_call in component.py); getting this wrong (e.g. raising this module's own ToolError instead) still
produces isError=True but replaces the message with a useless generic 'Error executing tool <name>'.
Mirrors dagster_instance_tools/report_tools_server/slack_chat_tools's own test_mcp_stdio.py."""
import os
import sys

import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

_COMPONENT_PATH = str(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "component.py"))


def _run(coro):
    import asyncio
    return asyncio.run(asyncio.wait_for(coro, 60))


def test_stdio_server_bad_args_is_a_real_isError_result_with_the_original_message(tmp_path):
    """notion-search with no tokens.json present under NOTION_MCP_OAUTH_DIR is a real, known
    NotionMcpAuthError (see _nmo_get_access_token) -- it must come back as a genuine isError
    result, not a quiet {"error": ...} a chat agent's failed-call accounting would miss. No
    network call: the auth check fails before ever reaching Notion's hosted MCP server."""
    params = StdioServerParameters(
        command=sys.executable, args=[_COMPONENT_PATH],
        env={**os.environ, "NOTION_MCP_OAUTH_DIR": str(tmp_path)},
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.call_tool("notion-search", {"query": "hello"})

    result = _run(go())
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    assert "No tokens found" in result.content[0].text


def test_stdio_server_lists_both_tools(tmp_path):
    """Sanity check on the same transport/process shape used by the isError test above -- the server
    starts cleanly and exposes both tools by default (no live Notion creds needed just to list)."""
    params = StdioServerParameters(
        command=sys.executable, args=[_COMPONENT_PATH],
        env={**os.environ, "NOTION_MCP_OAUTH_DIR": str(tmp_path)},
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.list_tools()

    result = _run(go())
    assert sorted(t.name for t in result.tools) == ["notion-fetch", "notion-search"]
