"""REAL MCP client-path tests for vercel_ai_gateway_agent.

A tiny real MCP server (written to tmp_path, run as a subprocess over stdio / streamable-http / sse) is driven through
the component's own MCP code. Drives the component's real `_run_agent` loop (MCP discovery + tool calls); only the paid model call is faked. Runs under whichever `mcp` major is installed (1.x or 2.x -- 2.x renamed
model attributes, the streamable-http client API and FastMCP; mocks hid that). Skipped when `mcp` is missing.
"""
import asyncio
import json
import logging
import os
import socket
import subprocess
import sys
import time
import types

import pytest

pytest.importorskip("mcp")

from .conftest import load_component_module  # noqa: E402

LOG = logging.getLogger("mcp_real_server_test")

SERVER_SRC = """
import sys
try:
    from mcp.server.mcpserver import MCPServer as Server  # mcp 2.x
except ImportError:
    from mcp.server.fastmcp import FastMCP as Server  # mcp 1.x

server = Server("mini")


@server.tool()
def echo(text: str, times: int = 1) -> dict:
    "Echo text back `times` times."
    return {"echo": text * times}


@server.tool()
def boom(reason: str) -> dict:
    "Always fails."
    raise ValueError(f"boom: {reason}")


def _bearer_app(app):
    async def wrapped(scope, receive, send):
        if scope["type"] == "http":
            hdrs = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            if hdrs.get("authorization") != "Bearer t0k":
                await send({"type": "http.response.start", "status": 401, "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await app(scope, receive, send)
    return wrapped


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "stdio":
        server.run(transport="stdio")
    else:
        import uvicorn
        app = server.streamable_http_app() if mode == "http" else server.sse_app()
        uvicorn.run(_bearer_app(app), host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
"""


def _child_env():
    # Keep inherited PYTHONPATH entries that exist as dirs (a per-major mcp install used to verify 1.x vs 2.x); this repo's
    # path contains ':' so PYTHONPATH can be split into junk entries.
    inherited = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p and os.path.isdir(p)]
    return {"PYTHONPATH": os.pathsep.join(inherited)}


@pytest.fixture(scope="module")
def server_py(tmp_path_factory):
    d = tmp_path_factory.mktemp("mini_mcp")
    p = d / "mini_mcp_server.py"
    p.write_text(SERVER_SRC)
    return str(p)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def stdio_cfg(server_py):
    return {"name": "mini", "type": "stdio", "command": [sys.executable, server_py, "stdio"], "env": _child_env()}


@pytest.fixture()
def http_cfg(server_py):
    procs = []

    def start(kind, headers):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        p = subprocess.Popen([sys.executable, server_py, kind, str(port)], stdout=subprocess.DEVNULL,
                             stderr=subprocess.PIPE, env={**os.environ, **_child_env()})
        procs.append(p)
        deadline = time.time() + 30
        while time.time() < deadline:
            if p.poll() is not None:
                raise RuntimeError(f"{kind} server died: {p.stderr.read().decode()[-2000:]}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                break
            except OSError:
                time.sleep(0.2)
        else:
            raise RuntimeError("server did not start")
        url = f"http://127.0.0.1:{port}" + ("/mcp" if kind == "http" else "/sse")
        return {"name": "mini", "type": "http" if kind == "http" else "sse", "url": url,
                "headers": headers}

    yield start
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=10)
        except Exception:
            p.kill()


class _Log:
    def __getattr__(self, _):
        return lambda *a, **k: None



def _call_kwargs(fn, cfg, **extra):
    import inspect
    cand = dict(log=_Log(), prompt="go", system_prompt=None, model="m", temperature=0.0, max_tokens=64,
                max_output_tokens=64, thinking_budget=None, max_iterations=3, mcp_servers=[cfg],
                request_timeout_seconds=30)
    cand.update(extra)
    params = inspect.signature(fn).parameters
    return {k: v for k, v in cand.items() if k in params}


def _tool_details(result):
    d = result["tool_call_details"][0]
    assert result["stopped_reason"] == "final_answer", result
    return d["result_preview"], d["is_error"]

def drive(mod, cfg, tool, args, monkeypatch):
    seen = {}
    state = {"n": 0}

    class Msg:
        def __init__(self, content, tool_calls=None):
            self.content, self.tool_calls = content, tool_calls

        def model_dump(self, exclude_none=True):
            return {"role": "assistant", "content": self.content}

    class _Completions:
        async def create(self, **kw):
            state["n"] += 1
            if state["n"] == 1:
                seen["tools"] = kw["tools"]
                tc = types.SimpleNamespace(id="c1", type="function",
                                           function=types.SimpleNamespace(name=f"mini__{tool}", arguments=json.dumps(args)))
                msg = Msg(None, [tc])
            else:
                msg = Msg("done")
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg, finish_reason="tool_calls" if msg.tool_calls else "stop")])

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.chat = types.SimpleNamespace(completions=_Completions())

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_KEY", "sk-fake")
    res = _run(mod._run_agent(**_call_kwargs(mod._run_agent, cfg, api_key_env_var="FAKE_KEY", base_url_env_var=None,
                                            organization_env_var=None, api_base_url="http://unused.invalid", model="openai/m")))
    out, is_error = _tool_details(res)
    return seen["tools"][0]["function"]["parameters"], out, is_error



def _run(coro, timeout=90):
    return asyncio.run(asyncio.wait_for(coro, timeout))


# ---- tests -----------------------------------------------------------------------------------------------------

def test_stdio_call_success_and_schema(mod, stdio_cfg, monkeypatch):
    schema, out, is_error = drive(mod, stdio_cfg, "echo", {"text": "hi", "times": 2}, monkeypatch)
    assert is_error is False
    assert "hihi" in out
    if schema is not None:  # components that forward the MCP tool's JSON schema to the model
        assert "text" in (schema.get("properties") or {}), f"input schema was dropped: {schema!r}"


def test_stdio_tool_error_is_reported_as_error(mod, stdio_cfg, monkeypatch):
    schema, out, is_error = drive(mod, stdio_cfg, "boom", {"reason": "x"}, monkeypatch)
    assert is_error is True, f"tool error reported as success: {out!r}"
    assert "boom" in out


@pytest.mark.parametrize("kind", ["http", "sse"])
def test_http_transports_with_bearer_header(mod, http_cfg, monkeypatch, kind):
    cfg = http_cfg(kind, {"Authorization": "Bearer t0k"})
    schema, out, is_error = drive(mod, cfg, "echo", {"text": "yo", "times": 2}, monkeypatch)
    assert is_error is False
    assert "yoyo" in out


def test_http_without_header_is_rejected(mod, http_cfg, monkeypatch):
    cfg = http_cfg("http", {})
    try:
        _, out, is_error = drive(mod, cfg, "echo", {"text": "yo"}, monkeypatch)
    except BaseException:  # noqa: BLE001 -- connection-level 401 surfaces as an exception group
        return
    assert is_error or "yo" not in out, "server requires a bearer token; the call must not succeed without it"


def test_result_helper_structured_only_and_snake_case(mod):
    """Synthetic result shapes (mcp 2.x snake_case / structured-content-only) against the helper's contract."""
    R = types.SimpleNamespace
    parts, err = mod._mcp_result_parts(R(content=[], structured_content={"a": 1}, is_error=True))
    assert err is True and json.loads(parts[0]) == {"a": 1}
    parts, err = mod._mcp_result_parts(R(content=[R(text="t")], structuredContent=None, isError=False))
    assert parts == ["t"] and err is False
    tool = R(name="x", inputSchema=None, input_schema={"type": "object", "properties": {"q": {}}})
    assert mod._mcp_field(tool, "inputSchema", None)["properties"] == {"q": {}}
    assert mod._mcp_field({"inputSchema": {"k": 1}}, "inputSchema", None) == {"k": 1}
