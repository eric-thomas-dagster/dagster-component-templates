"""Launch component.py as a real stdio MCP server and call it through a real MCP client -- exactly how
agentic_pipeline/slack_chat_tools etc. spawn a stdio server in production. Only checks the bad-arguments
failure path: `main()` requires GITHUB_PR_TOOLS_REPO just to construct the client, but `read_file` on a
path matching exclude_paths is refused before any GitHub API call, so this test needs no live repo or
token."""
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
    """A refused call (read_file on an excluded path) must come back as a REAL isError result, not a quiet
    non-error {"error": ...} dict an agent loop's failed-call accounting would miss -- see _mcp_call in
    component.py."""
    env = {
        **os.environ,
        "GITHUB_PR_TOOLS_REPO": "example-org/example-repo",
        "GITHUB_PR_TOOLS_EXCLUDE_PATHS": ".env",
    }

    async def body(s):
        return await s.call_tool("read_file", {"path": ".env"})

    result = _run(body, env=env)
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    assert "is excluded (exclude_paths)" in result.content[0].text
