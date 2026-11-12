"""Launch component.py as a real stdio MCP server, spawned with NO inherited environment (env={} plus only what's
explicitly passed), and call it through a real MCP client -- exactly how agentic_pipeline's mcp_servers config
spawns a stdio server in production, and exactly the scenario `mcp_extra_env_vars`/`live_env_passthrough` exist for
elsewhere in this file's sibling tests."""
import asyncio
import json
import sys

import pytest

mcp = pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from .conftest import HERE  # noqa: E402

SERVER = str(HERE.parent / "component.py")


def _run(coro_fn, env, timeout=60):
    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await coro_fn(s)

    return asyncio.run(asyncio.wait_for(go(), timeout))


def _json(res):
    return json.loads(res.content[0].text)


def test_stdio_server_with_zero_inherited_env_lists_and_serves_all_tools():
    """env={} -- not os.environ, not even PATH -- the server needs none of its own env vars to do anything at all
    (no API keys, no DAGSTER_HOME: every tool here is pure introspection over files already on disk)."""
    async def body(s):
        names = sorted(t.name for t in (await s.list_tools()).tools)
        ops = _json(await s.call_tool("list_ops", {}))
        return names, ops

    names, ops = _run(body, env={})
    assert names == sorted([
        "list_ops", "describe_op", "list_examples", "get_example", "validate_pipeline", "preview_pipeline",
    ])
    assert len(ops["ops"]) == 26


def test_stdio_server_describe_op_and_validate_pipeline_with_zero_inherited_env():
    async def body(s):
        described = _json(await s.call_tool("describe_op", {"op": "llm_call"}))
        valid = _json(await s.call_tool("validate_pipeline", {"yaml": (
            "asset_name_prefix: demo\nsource: {kind: literal, text: hi}\n"
            "steps:\n  - id: d\n    op: llm_call\n    model: gpt-4o-mini\n"
            "    api_key_env_var: OPENAI_API_KEY\n    prompt_template: '{text}'\n"
            "outputs: {assets: [d]}\n"
        )}))
        return described, valid

    described, valid = _run(body, env={})
    assert described["op"] == "llm_call" and "properties" in described["schema"]
    assert valid == {"valid": True, "errors": []}


def test_stdio_server_allow_env_restricts_tools():
    async def body(s):
        return sorted(t.name for t in (await s.list_tools()).tools)

    names = _run(body, env={"AGENTIC_PIPELINE_AUTHORING_TOOLS_ALLOW": "list_ops,describe_op"})
    assert names == ["describe_op", "list_ops"]
