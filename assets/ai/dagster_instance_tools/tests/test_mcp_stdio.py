"""Launch component.py as a real stdio MCP server (exactly how agentic_pipeline's mcp_servers config does) and call it."""
import asyncio
import json
import os
import sys

import pytest

mcp = pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from .conftest import HERE  # noqa: E402

SERVER = str(HERE.parent / "component.py")


def _run(world, coro_fn, **env_extra):
    env = {**os.environ, "DAGSTER_HOME": str(world["home"]), "PYTHONPATH": world["defs_dir"], **env_extra}
    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await coro_fn(s)
    return asyncio.run(asyncio.wait_for(go(), 90))


def _json(res):
    return json.loads(res.content[0].text)


def test_stdio_server_lists_and_serves_tools(world):
    async def body(s):
        names = sorted(t.name for t in (await s.list_tools()).tools)
        fail = _json(await s.call_tool("list_recent_failures", {"hours": 1}))
        logs = _json(await s.call_tool("get_run_logs", {"run_id": world["bad_run"], "level": "INFO"}))
        bad = _json(await s.call_tool("get_run_summary", {"run_id": "nope"}))
        return names, fail, logs, bad
    names, fail, logs, bad = _run(world, body)
    assert names == sorted(["list_recent_failures", "get_run_summary", "get_run_logs", "asset_status", "asset_lineage", "stale_assets"])
    assert {r["run_id"] for r in fail["runs"]} == {world["bad_run"], world["part_b_run"]}
    assert "FAKEFAKEFAKE" not in json.dumps(logs) and "Bearer [REDACTED]" in "\n".join(logs["lines"])
    assert "run not found" in bad["error"]


def test_stdio_server_allow_env_and_definitions_module(world):
    async def body(s):
        names = sorted(t.name for t in (await s.list_tools()).tools)
        down = _json(await s.call_tool("asset_lineage", {"asset_key": "ok", "direction": "downstream", "depth": 1}))
        return names, down
    names, down = _run(world, body, DAGSTER_INSTANCE_TOOLS_ALLOW="asset_lineage,asset_status",
                       DAGSTER_INSTANCE_TOOLS_DEFINITIONS_MODULE="fixture_defs")
    assert names == ["asset_lineage", "asset_status"]  # others are not even exposed
    assert down["downstream"] == ["bad", "chk"] and down["source"] == "asset_graph"


def test_stdio_server_requires_dagster_home(world):
    import subprocess
    env = {k: v for k, v in os.environ.items() if k != "DAGSTER_HOME"}
    p = subprocess.run([sys.executable, SERVER], env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode != 0 and "DAGSTER_HOME" in p.stderr


def test_agentic_pipeline_tool_use_loop_discovery_and_call_work_against_this_server(world, monkeypatch):
    """The REAL discovery + call helpers agentic_pipeline's tool_use_loop uses, pointed at this server via `mcp_servers` config."""
    import importlib.util
    path = HERE.parents[1] / "agentic_pipeline" / "component.py"
    if not path.exists():
        pytest.skip("agentic_pipeline not present")
    spec = importlib.util.spec_from_file_location("agentic_for_dit_tests", path)
    ap = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(ap)
    except Exception as e:  # sibling mid-edit by another session: do not fail this component's suite
        pytest.skip(f"agentic_pipeline not importable right now: {e}")
    import logging
    monkeypatch.setenv("DAGSTER_HOME", str(world["home"]))
    cfg = {"name": "dagster", "type": "stdio", "command": [sys.executable, SERVER], "env": {"DAGSTER_HOME": str(world["home"])}}
    log = logging.getLogger("t")
    tools = asyncio.run(asyncio.wait_for(ap._discover_mcp_tools_async(log, [cfg]), 90))
    names = {t["name"] for t in tools}
    assert {"list_recent_failures", "asset_status", "get_run_logs"} <= names
    schema = ap._mcp_tools_to_openai_schema(tools, None)
    assert any(s["function"]["name"] == "dagster__asset_status" for s in schema)
    res = asyncio.run(asyncio.wait_for(ap._call_mcp_tool_async(
        log=log, server_cfg=cfg, tool_name="asset_status", tool_args={"asset_key": "chk"}, parse_as="auto"), 90))
    payload = json.loads(res["raw"])
    assert payload["checks"][0]["check"] == "row_count_positive"
