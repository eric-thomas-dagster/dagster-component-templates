"""`type: fastmcp` client transport against a REAL fastmcp server (fixtures/fastmcp_server.py) over stdio (inline
mcp.json-style `config`), streamable-http and sse. Skipped unless the `fastmcp` package is installed (it pulls its own
`mcp`, so run it with e.g. PYTHONPATH=<site with fastmcp>). Verified on fastmcp 4.1.3 (+ mcp 2.3.0)."""
import asyncio
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time

import pytest

pytest.importorskip("fastmcp")

from .conftest import new_state, tool_call  # noqa: E402

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
LOG = logging.getLogger("fastmcp_client")


def _run(coro, timeout=90):
    return asyncio.run(asyncio.wait_for(coro, timeout))


@pytest.fixture(scope="module")
def srv_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("fm_fix")
    for f in ("fastmcp_server.py", "raw_mcp_server.py"):
        shutil.copy(FIXTURES / f, d / f)
    return d


def _stdio_env(extra=None):
    # a fastmcp-config stdio server gets ONLY the env listed here (no inheritance), so hand it what python needs
    env = {k: os.environ[k] for k in ("PATH", "HOME", "PYTHONPATH") if k in os.environ}
    env.update(extra or {})
    return env


def _stdio_cfg(srv_dir, extra_env=None, **extra):
    return {"name": "fm", "type": "fastmcp", "config": {"mcpServers": {"fm": {
        "command": sys.executable, "args": [str(srv_dir / "fastmcp_server.py"), "stdio"], "env": _stdio_env(extra_env)}}}, **extra}


def _call(mod, cfg, tool, args=None, parse_as="auto", **kw):
    return _run(mod._call_mcp_tool_async(log=LOG, server_cfg=cfg, tool_name=tool, tool_args=args or {}, parse_as=parse_as, **kw))


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def http_server(srv_dir):
    procs = []

    def start(kind):
        port = _free_port()
        p = subprocess.Popen([sys.executable, str(srv_dir / "fastmcp_server.py"), kind, str(port)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        procs.append(p)
        deadline = time.time() + 40
        while time.time() < deadline:
            if p.poll() is not None:
                raise RuntimeError(f"{kind} server died: {p.stderr.read().decode()[-2000:]}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                return f"http://127.0.0.1:{port}" + ("/mcp" if kind == "http" else "/sse")
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("fastmcp server did not start")

    yield start
    for p in procs:
        p.terminate()
        try:
            p.wait(5)
        except Exception:  # noqa: BLE001
            p.kill()


# ── stdio via inline config ─────────────────────────────────────────────────────────────────────────────────────

def test_stdio_discovery_schema_and_structured_call(mod, srv_dir):
    cfg = _stdio_cfg(srv_dir)
    tools = _run(mod._discover_mcp_tools_async(LOG, [cfg]))
    by = {t["name"]: t for t in tools}
    assert {"echo", "boom", "plain", "multi", "big", "sleep", "seen_headers", "env", "pid"} <= set(by)
    assert by["echo"]["input_schema"]["properties"]["text"]["type"] == "string"  # fastmcp 4 returns snake_case input_schema
    assert by["echo"]["input_schema"]["required"] == ["text"]
    assert "Echo text" in by["echo"]["description"]
    res = _call(mod, cfg, "echo", {"text": "ab", "times": 2})
    assert res["value"] == {"echo": "abab"} and res["kind"] == "json" and res["is_error"] is False
    assert res["structured_content"] == {"echo": "abab"}
    assert _call(mod, cfg, "plain", {"text": "z"})["raw"] == "plain:z"


def test_tool_error_is_an_is_error_result_like_the_other_transports(mod, srv_dir):
    # fastmcp's Client.call_tool RAISES ToolError by default; the sdk transports return an isError result. The shim must
    # present one contract (this used to surface as an exception with the fastmcp-specific type)
    res = _call(mod, _stdio_cfg(srv_dir), "boom", {"reason": "x"}, parse_as="text")
    assert res["is_error"] is True and "boom: x" in res["raw"]


def test_validate_input_schema_uses_the_real_server_schema(mod, srv_dir):
    cfg = _stdio_cfg(srv_dir)
    with pytest.raises(Exception) as ei:
        _call(mod, cfg, "echo", {"times": 2}, validate_input_schema=True)
    assert "'text' is a required property" in repr(ei.value)
    assert _call(mod, cfg, "echo", {"text": "q"}, validate_input_schema=True)["value"] == {"echo": "q"}
    with pytest.raises(ValueError, match="no tool named 'nope'"):
        _call(mod, cfg, "nope", {}, validate_input_schema=True)


def test_image_part_is_a_placeholder_not_a_base64_dump(mod, srv_dir):
    res = _call(mod, _stdio_cfg(srv_dir), "multi", parse_as="text")
    assert res["raw"].startswith("part one") and res["raw"].endswith("part last")
    assert "[image omitted: image/png" in res["raw"] and "iVBOR" not in res["raw"]
    assert [p["type"] for p in res["content_parts"]] == ["text", "image", "text"]


def test_large_text_and_unknown_tool_and_timeout(mod, srv_dir):
    cfg = _stdio_cfg(srv_dir)
    assert len(_call(mod, cfg, "big", {"n": 300_000}, parse_as="text")["raw"]) == 300_000
    # an unknown tool: fastmcp reports it as an error result (not a protocol error) -- still a clean is_error, never a hang
    res = _call(mod, cfg, "nope", {}, parse_as="text")
    assert res["is_error"] is True and "nope" in res["raw"]
    t0 = time.time()
    with pytest.raises(mod.MCPServerError, match="timed out after 2s"):
        _call(mod, dict(cfg, timeout_seconds=2), "sleep", {"seconds": 60})
    assert time.time() - t0 < 20


def test_stdio_server_env_is_exactly_the_config_env(mod, srv_dir, monkeypatch):
    monkeypatch.setenv("PARENT_ONLY_SECRET", "nope")
    cfg = _stdio_cfg(srv_dir, extra_env={"FM_ONLY": "yes"})
    assert _call(mod, cfg, "env", {"name": "FM_ONLY"}, parse_as="text")["raw"] == "yes"
    assert _call(mod, cfg, "env", {"name": "PARENT_ONLY_SECRET"}, parse_as="text")["raw"] == "<unset>"  # not inherited (stricter than type: stdio)


def test_stdio_server_process_is_not_left_running(mod, srv_dir):
    cfg = _stdio_cfg(srv_dir)
    pid = int(_call(mod, cfg, "pid", parse_as="text")["raw"])
    deadline = time.time() + 10
    while time.time() < deadline and subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True).stdout.strip():
        time.sleep(0.2)
    assert subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True).stdout.strip() == ""


def test_pagination_through_a_fastmcp_client(mod, srv_dir):
    raw = {"name": "pg", "type": "fastmcp", "config": {"mcpServers": {"pg": {
        "command": sys.executable, "args": [str(srv_dir / "raw_mcp_server.py"), "--page-size", "4", "--n-tools", "21", "--only-extra"],
        "env": _stdio_env()}}}}
    tools = _run(mod._discover_mcp_tools_async(LOG, [raw]))
    assert [t["name"] for t in tools] == [f"tool_{i:03d}" for i in range(21)]


def test_crashed_server_names_the_server(mod, srv_dir):
    raw = {"name": "flaky", "type": "fastmcp", "timeout_seconds": 30, "config": {"mcpServers": {"flaky": {
        "command": sys.executable, "args": [str(srv_dir / "raw_mcp_server.py")], "env": _stdio_env()}}}}
    t0 = time.time()
    with pytest.raises(mod.MCPServerError, match="'flaky'"):
        _call(mod, raw, "crash")
    assert time.time() - t0 < 25


# ── http / sse with auth ───────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("kind", ["http", "sse"])
def test_remote_with_bearer_and_literal_headers(mod, http_server, kind, monkeypatch):
    url = http_server(kind)
    monkeypatch.setenv("FM_TOKEN", "t0k")
    cfg = {"name": "fm", "type": "fastmcp", "url": url, "bearer_env": "FM_TOKEN", "headers": {"X-Extra": "hello"}}
    tools = _run(mod._discover_mcp_tools_async(LOG, [cfg]))
    assert {"echo", "seen_headers"} <= {t["name"] for t in tools}
    assert _call(mod, cfg, "echo", {"text": "hi"})["value"] == {"echo": "hi"}
    if kind == "http":  # the header-echo tool needs a request context (streamable-http)
        seen = _call(mod, cfg, "seen_headers")["value"]
        assert seen == {"authorization": "Bearer t0k", "x-extra": "hello"}


def test_remote_with_headers_env_instead_of_bearer(mod, http_server, monkeypatch):
    url = http_server("http")
    monkeypatch.setenv("FM_AUTH", "Bearer t0k")
    cfg = {"name": "fm", "type": "fastmcp", "url": url, "headers_env": {"Authorization": "FM_AUTH"}}
    assert _call(mod, cfg, "plain", {"text": "a"}, parse_as="text")["raw"] == "plain:a"


@pytest.mark.parametrize("kind", ["http", "sse"])
def test_remote_without_credentials_fails_with_the_server_named(mod, http_server, kind):
    url = http_server(kind)
    with pytest.raises(mod.MCPServerError) as ei:
        _call(mod, {"name": "fm", "type": "fastmcp", "url": url, "timeout_seconds": 30}, "echo", {"text": "x"})
    assert "'fm'" in str(ei.value)


def test_missing_bearer_env_is_silently_unauthenticated_but_fails_loudly_at_the_server(mod, http_server, monkeypatch):
    monkeypatch.delenv("FM_NOPE", raising=False)
    url = http_server("http")
    with pytest.raises(mod.MCPServerError):
        _call(mod, {"name": "fm", "type": "fastmcp", "url": url, "bearer_env": "FM_NOPE", "timeout_seconds": 30}, "echo", {"text": "x"})


def test_secret_token_never_in_logs_or_errors(mod, http_server, monkeypatch, capfd, caplog):
    secret = "tok-SECRET-7731"
    monkeypatch.setenv("FM_TOKEN", secret)  # wrong token on purpose: the 401 path must not echo it
    caplog.set_level(logging.DEBUG)
    url = http_server("http")
    with pytest.raises(mod.MCPServerError) as ei:
        _call(mod, {"name": "fm", "type": "fastmcp", "url": url, "bearer_env": "FM_TOKEN", "timeout_seconds": 30}, "echo", {"text": "x"})
    cap = capfd.readouterr()
    assert secret not in cap.out + cap.err + caplog.text + repr(ei.value)


# ── policy applies to the fastmcp transport too ─────────────────────────────────────────────────────────────────

@pytest.fixture
def activate(mod):
    tokens = []

    def _go(cfg):
        tokens.append(mod._ACTIVE_POLICY.set(mod._Policy(cfg, {})))

    yield _go
    for t in reversed(tokens):
        mod._ACTIVE_POLICY.reset(t)


def test_policy_network_allow_hosts_is_enforced_for_fastmcp_urls(mod, activate):
    activate({"network": {"allow_hosts": ["api.allowed.example"]}})
    with pytest.raises(mod.PolicyViolation):
        _call(mod, {"name": "fm", "type": "fastmcp", "url": "http://127.0.0.1:9/mcp"}, "echo", {"text": "x"})
    with pytest.raises(mod.PolicyViolation):  # ...including a url inside an inline mcp.json config
        _call(mod, {"name": "fm", "type": "fastmcp", "config": {"mcpServers": {"x": {"url": "http://127.0.0.1:9/mcp"}}}}, "echo", {"text": "x"})
    with pytest.raises(mod.PolicyViolation):
        _run(mod._discover_mcp_tools_async(LOG, [{"name": "fm", "type": "fastmcp", "url": "http://evil.example/mcp"}]))


def test_fastmcp_needs_url_or_config(mod):
    with pytest.raises(ValueError, match="neither `url` nor `config`"):
        _call(mod, {"name": "fm", "type": "fastmcp"}, "echo", {})


# ── inside a tool_use_loop ──────────────────────────────────────────────────────────────────────────────────────

def test_tool_use_loop_over_fastmcp(mod, srv_dir, fake_litellm, ctx):
    cfg = _stdio_cfg(srv_dir)
    fake_litellm.reply("", tool_calls=[tool_call("fm__echo", {"text": "q", "times": 3}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("fm__boom", {"reason": "zz"}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
    state = new_state()
    mod._run_step({"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "max_iterations": 5, "mcp_servers": [cfg]}, state, ctx)
    out = state["agent"]
    assert out["stop_reason"] == "finalize_called" and out["n_tool_calls"] == 2
    tool_msgs = [m["content"] for m in fake_litellm.calls[-1]["messages"] if m["role"] == "tool"]
    assert '"echo":"qqq"' in tool_msgs[0]
    assert tool_msgs[1].startswith("ERROR: tool reported failure:") and "boom: zz" in tool_msgs[1]
