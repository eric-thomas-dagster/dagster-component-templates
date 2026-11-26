"""Launch component.py as a real stdio MCP server and call it through a real MCP client -- exactly how
agentic_pipeline/slack_chat_tools etc. spawn a stdio server in production. Only checks the bad-arguments
failure path: `main()` requires a real-looking deployment_url + a non-empty token env var just to construct
the client, but `check_dagster_api` with an empty `names` list fails validation before any network call, so
this test needs no live Dagster+ deployment."""
import asyncio
import os
import sys

import pytest

mcp = pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

SERVER = str(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "component.py"))


def _run(coro_fn, env, timeout=60):
    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await coro_fn(s)

    return asyncio.run(asyncio.wait_for(go(), timeout))


def test_stdio_server_bad_args_is_a_real_isError_result_with_the_original_message():
    """A bad-arguments failure (empty `names`) must come back as a REAL isError result, not a quiet non-error
    {"error": ...} dict an agent loop's failed-call accounting would miss -- see _mcp_call in component.py."""
    env = {
        **os.environ,
        "DAGSTER_EVIDENCE_TOOLS_DEPLOYMENT_URL": "https://example.dagster.cloud/prod",
        "DAGSTER_CLOUD_API_TOKEN": "dummy-token-not-used-by-this-test",
    }

    async def body(s):
        return await s.call_tool("check_dagster_api", {"names": []})

    result = _run(body, env=env)
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    assert "check_dagster_api requires" in result.content[0].text
