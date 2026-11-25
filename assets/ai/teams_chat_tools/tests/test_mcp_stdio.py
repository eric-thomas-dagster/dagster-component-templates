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


def test_stdio_server_bad_args_is_a_real_isError_result_with_the_original_message():
    """get_thread with no team configured (TEAMS_CHAT_TOOLS_TEAM_ID left unset) is a real, known
    TeamsChatTools failure (see t_get_thread's own ToolError) -- it must come back as a genuine
    isError result, not a quiet {"error": ...} a chat agent's failed-call accounting would miss."""
    params = StdioServerParameters(
        command=sys.executable, args=[_COMPONENT_PATH],
        env={
            **os.environ,
            "TEAMS_TENANT_ID": "tid", "TEAMS_CLIENT_ID": "cid", "TEAMS_CLIENT_SECRET": "sec", "TEAMS_REFRESH_TOKEN": "seed",
            "TEAMS_CHAT_TOOLS_TEAM_ID": "", "TEAMS_CHAT_TOOLS_CHANNEL_ID": "", "TEAMS_CHAT_TOOLS_THREAD_ID": "",
        },
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.call_tool("get_thread", {})

    result = _run(go())
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    assert "no team configured" in result.content[0].text


def test_stdio_server_lists_the_tool():
    """Sanity check on the same transport/process shape used by the isError test above -- the server
    starts cleanly and exposes get_thread (no live Teams creds needed just to list)."""
    params = StdioServerParameters(
        command=sys.executable, args=[_COMPONENT_PATH],
        env={
            **os.environ,
            "TEAMS_TENANT_ID": "tid", "TEAMS_CLIENT_ID": "cid", "TEAMS_CLIENT_SECRET": "sec", "TEAMS_REFRESH_TOKEN": "seed",
        },
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.list_tools()

    result = _run(go())
    assert [t.name for t in result.tools] == ["get_thread"]
