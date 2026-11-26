"""Real stdio subprocess round trip (same transport agentic_pipeline/slack_chat_tools etc. use)
-- not just calling the registered function in-process -- since the SDK's tool dispatch only
preserves a raised exception's own message when it recognizes the exception as ITS OWN
ToolError (see _mcp_call in component.py); getting this wrong (e.g. raising some other
same-named ToolError) still produces isError=True but replaces the message with a useless
generic 'Error executing tool <name>'. Mirrors dagster_instance_tools/tests/test_mcp_stdio.py
and report_tools_server/tests/test_report_tools_server.py's identical check.

Uses token_store='state' (HOSTED_MCP_ACCESS_TOKEN) so no real OAuth sign-in is needed, and points
HOSTED_MCP_URL at a local port nothing listens on so the upstream call fails fast and
deterministically with a real connection error (ECONNREFUSED), with HOSTED_MCP_MAX_RETRIES=0 so
the failure surfaces on the first attempt with no retry/backoff delay."""
import asyncio
import os
import sys

import pytest


def test_stdio_server_upstream_failure_is_a_real_isError_result_with_the_original_message():
    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    server = str(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "component.py"))
    params = StdioServerParameters(
        command=sys.executable,
        args=[server],
        env={
            **os.environ,
            "HOSTED_MCP_URL": "http://127.0.0.1:1/mcp",  # nothing listens here -- fails fast
            "HOSTED_MCP_ACCESS_TOKEN": "AT123",
            "HOSTED_MCP_ALLOW": "get_issue",
            "HOSTED_MCP_MAX_RETRIES": "0",
            "HOSTED_MCP_MAX_REQUESTS_PER_SECOND": "1000",
        },
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                names = sorted(t.name for t in (await s.list_tools()).tools)
                bad = await s.call_tool("get_issue", {"arguments": {"id": "ABC-123"}})
                return names, bad

    names, bad = asyncio.run(asyncio.wait_for(go(), 60))
    assert names == ["get_issue"]
    # A tool-level failure (unreachable upstream) must come back as a REAL isError result, not a
    # quiet non-error {"error": ...} dict an agent loop's failed-call accounting would miss.
    assert getattr(bad, "is_error", None) or getattr(bad, "isError", None)
    assert "get_issue failed" in bad.content[0].text
