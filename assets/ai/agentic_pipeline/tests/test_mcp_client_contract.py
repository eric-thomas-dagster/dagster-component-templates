"""Real-server contract tests for agentic_pipeline's MCP *client* helpers (stdio / http / sse), against a tiny real server
(fixtures/mini_mcp_server.py) run as a subprocess. Runs under whichever `mcp` major is installed (1.x or 2.x; the
2.x SDK renamed model attributes and the streamable-http client API -- mocks hid that). Skipped when `mcp` is missing."""
import asyncio
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time

import pytest

pytest.importorskip("mcp")

FIXTURE = pathlib.Path(__file__).resolve().parent / "fixtures" / "mini_mcp_server.py"
LOG = logging.getLogger("mcp_contract")


def _run(coro, timeout=60):
    return asyncio.run(asyncio.wait_for(coro, timeout))


@pytest.fixture(scope="module")
def server_py(tmp_path_factory):
    d = tmp_path_factory.mktemp("mini_mcp")  # clean dir: PYTHONPATH/cwd must not contain this repo's ':' path
    dst = d / "mini_mcp_server.py"
    shutil.copy(FIXTURE, dst)
    return str(dst)


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def http_server(server_py):
    procs = []

    def start(kind):
        port = _free_port()
        p = subprocess.Popen([sys.executable, server_py, kind, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        procs.append(p)
        deadline = time.time() + 30
        while time.time() < deadline:
            if p.poll() is not None:
                raise RuntimeError(f"{kind} server died: {p.stderr.read().decode()[-2000:]}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                return f"http://127.0.0.1:{port}" + ("/mcp" if kind == "http" else "/sse")
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("server did not start")

    yield start
    for p in procs:
        p.terminate()
        try:
            p.wait(5)
        except Exception:
            p.kill()


def _stdio_cfg(server_py):
    return {"name": "mini", "type": "stdio", "command": [sys.executable, server_py, "stdio"]}


def test_stdio_discovery_schema_and_call(mod, server_py):
    cfg = _stdio_cfg(server_py)
    tools = _run(mod._discover_mcp_tools_async(LOG, [cfg]))
    by = {t["name"]: t for t in tools}
    assert set(by) == {"echo", "boom"}
    # the real input schema must survive (mcp 2.x renamed inputSchema -> input_schema; a naive getattr silently drops it)
    assert by["echo"]["input_schema"]["properties"]["text"]["type"] == "string"
    assert by["echo"]["input_schema"]["required"] == ["text"]
    assert "Echo text" in by["echo"]["description"]
    schema = mod._mcp_tools_to_openai_schema(tools, None)
    assert {s["function"]["name"] for s in schema} == {"mini__echo", "mini__boom"}
    res = _run(mod._call_mcp_tool_async(log=LOG, server_cfg=cfg, tool_name="echo", tool_args={"text": "ab", "times": 2}, parse_as="auto"))
    assert res["value"] == {"echo": "abab"} and res["is_error"] is False and res["kind"] == "json"


def test_stdio_tool_error_sets_is_error(mod, server_py):
    res = _run(mod._call_mcp_tool_async(log=LOG, server_cfg=_stdio_cfg(server_py), tool_name="boom", tool_args={"reason": "x"}, parse_as="text"))
    assert res["is_error"] is True and "boom" in res["raw"]  # mcp 2.x: attribute is is_error, not isError (error text differs per major)


def test_stdio_validate_input_schema_rejects_bad_args(mod, server_py):
    with pytest.raises(Exception) as ei:
        _run(mod._call_mcp_tool_async(log=LOG, server_cfg=_stdio_cfg(server_py), tool_name="echo", tool_args={"times": 2},
                                      parse_as="auto", validate_input_schema=True))
    assert "'text' is a required property" in repr(ei.value)  # (anyio wraps it in ExceptionGroups) # the real server-declared schema was fetched and enforced
    ok = _run(mod._call_mcp_tool_async(log=LOG, server_cfg=_stdio_cfg(server_py), tool_name="echo", tool_args={"text": "z"},
                                       parse_as="auto", validate_input_schema=True))
    assert ok["value"] == {"echo": "z"}


@pytest.mark.parametrize("kind,ttype", [("http", "http"), ("sse", "sse")])
def test_remote_transports_with_auth_headers(mod, http_server, kind, ttype, monkeypatch):
    url = http_server(kind)
    monkeypatch.setenv("MINI_TOKEN", "Bearer t0k")
    cfg = {"name": "mini", "type": ttype, "url": url, "headers_env": {"Authorization": "MINI_TOKEN"}}
    tools = _run(mod._discover_mcp_tools_async(LOG, [cfg]))
    assert {t["name"] for t in tools} == {"echo", "boom"}
    res = _run(mod._call_mcp_tool_async(log=LOG, server_cfg=cfg, tool_name="echo", tool_args={"text": "hi"}, parse_as="auto"))
    assert res["value"] == {"echo": "hi"}


@pytest.mark.parametrize("kind,ttype", [("http", "http"), ("sse", "sse")])
def test_remote_transports_reject_missing_auth(mod, http_server, kind, ttype):
    url = http_server(kind)
    with pytest.raises(BaseException):
        _run(mod._discover_mcp_tools_async(LOG, [{"name": "mini", "type": ttype, "url": url}]), timeout=30)
