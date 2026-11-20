"""Real, committed tests for HostedMcpServerComponent / HostedMcpProxyTools.

Mocks only the unavoidable external call (`_hmo_call_tool_async`, the actual network hop to the
hosted MCP server), per this repo's "mock only the paid/external call" convention.
"""
import asyncio
import sys

import pytest


def test_build_defs_registers_stdio_resource_with_no_allow_by_default(mod, tmp_path):
    component = mod.HostedMcpServerComponent(resource_key="mcp", url="https://mcp.example.com/mcp", oauth_dir=str(tmp_path))
    defs = component.build_defs(context=None)
    resource = defs.resources["mcp"]
    assert resource.type == "stdio"
    assert resource.command[0] == sys.executable
    assert resource.env["HOSTED_MCP_URL"] == "https://mcp.example.com/mcp"
    assert resource.env["HOSTED_MCP_OAUTH_DIR"] == str(tmp_path)
    assert "HOSTED_MCP_ALLOW" not in resource.env  # empty by default -- nothing exposed until chosen


def test_build_defs_allow_list_and_soft_rate_limit_pattern_pass_through(mod, tmp_path):
    component = mod.HostedMcpServerComponent(
        url="https://mcp.example.com/mcp", oauth_dir=str(tmp_path),
        allow=["get_issue", "create_comment"], soft_rate_limit_pattern="slow down",
    )
    defs = component.build_defs(context=None)
    resource = defs.resources["mcp"]
    assert resource.env["HOSTED_MCP_ALLOW"] == "get_issue,create_comment"
    assert resource.env["HOSTED_MCP_SOFT_RATE_LIMIT_PATTERN"] == "slow down"


# --- HostedMcpProxyTools ----------------------------------------------------------------------

def test_proxy_tools_disallowed_tool_is_an_error_not_an_exception(mod, tmp_path):
    tools = mod.HostedMcpProxyTools("https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token", allow=["get_issue"])
    result = tools.call("delete_everything", {})
    assert "error" in result
    assert "not enabled" in result["error"]


def test_proxy_tools_call_returns_text_on_success(mod, monkeypatch, tmp_path):
    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        return f"result for {tool_name} {arguments}"

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    tools = mod.HostedMcpProxyTools("https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token", allow=["get_issue"], max_requests_per_second=1000)
    result = tools.call("get_issue", {"id": "ABC-123"})
    assert "result for get_issue" in result["text"]


def test_proxy_tools_auth_error_becomes_error_dict(mod, monkeypatch, tmp_path):
    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        raise mod.HostedMcpAuthError("sign in again")

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    tools = mod.HostedMcpProxyTools("https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token", allow=["get_issue"], max_requests_per_second=1000)
    result = tools.call("get_issue", {})
    assert "sign in again" in result["error"]


def test_proxy_tools_retries_on_real_http_429_and_tracks_hits(mod, monkeypatch, tmp_path):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)
    attempts = {"n": 0}

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("HTTP 429 Too Many Requests")
        return "ok"

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    tools = mod.HostedMcpProxyTools("https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token", allow=["get_issue"], max_requests_per_second=1000)
    result = tools.call("get_issue", {})
    assert result["text"] == "ok"
    assert tools.rate_limit_hits == 1


def test_proxy_tools_retries_on_soft_rate_limit_text_with_custom_pattern(mod, monkeypatch, tmp_path):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)
    responses = iter(["please slow down a bit", "ok"])

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        return next(responses)

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    tools = mod.HostedMcpProxyTools(
        "https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token",
        allow=["get_issue"], max_requests_per_second=1000, soft_rate_limit_pattern="slow down",
    )
    result = tools.call("get_issue", {})
    assert result["text"] == "ok"
    assert tools.rate_limit_hits == 1


def test_proxy_tools_rate_limited_past_max_retries_is_an_error_dict(mod, monkeypatch, tmp_path):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        raise RuntimeError("429")

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    tools = mod.HostedMcpProxyTools("https://mcp.example.com/mcp", str(tmp_path), "https://auth.example.com/token", allow=["get_issue"], max_requests_per_second=1000, max_retries=2)
    result = tools.call("get_issue", {})
    assert "error" in result


# --- check_token_health -----------------------------------------------------------------------

def test_check_token_health_missing_client_info(mod, tmp_path):
    result = mod.check_token_health(str(tmp_path))
    assert result["healthy"] is False
    assert "Sign in first" in result["message"]


def test_check_token_health_rejected_refresh_token(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    mod._hmo_atomic_write(mod._hmo_tokens_path(oauth_dir), {"access_token": "old", "refresh_token": "bad", "expires_at": 0})
    mod._hmo_atomic_write(mod._hmo_client_info_path(oauth_dir), {"client_id": "cid", "token_endpoint": "https://auth.example.com/token"})

    class _Resp:
        status_code = 401

        def raise_for_status(self):
            pass

        def json(self):
            return {}

    monkeypatch.setattr("requests.post", lambda *a, **k: _Resp())
    result = mod.check_token_health(oauth_dir)
    assert result["healthy"] is False
    assert "sign in again" in result["message"].lower()


def test_check_token_health_valid_token(mod, tmp_path):
    oauth_dir = str(tmp_path)
    import time
    mod._hmo_atomic_write(mod._hmo_tokens_path(oauth_dir), {"access_token": "AT", "refresh_token": "RT", "expires_at": time.time() + 3600})
    mod._hmo_atomic_write(mod._hmo_client_info_path(oauth_dir), {"client_id": "cid", "token_endpoint": "https://auth.example.com/token"})
    result = mod.check_token_health(oauth_dir)
    assert result["healthy"] is True


# --- build_mcp_server: dynamic <name>(arguments: dict) proxy tools ------------------------------

class _FakeTools:
    def __init__(self, names):
        self._names = names
        self.calls = []

    def tool_names(self):
        return self._names

    def call(self, tool, args=None):
        self.calls.append((tool, args))
        return {"text": f"called {tool}"}


def test_build_mcp_server_registers_one_tool_per_allowed_name(mod):
    tools = _FakeTools(["get_issue", "create_comment"])
    mcp = mod.build_mcp_server(tools)

    async def check():
        return await mcp.list_tools()

    listed = asyncio.run(check())
    assert {t.name for t in listed} == {"get_issue", "create_comment"}
    # free-form passthrough schema -- not flattened kwargs matching the upstream tool's own shape
    schema = listed[0].input_schema
    assert schema["properties"]["arguments"]["anyOf"][0]["additionalProperties"] is True


def test_build_mcp_server_proxy_tool_forwards_arguments_dict(mod):
    tools = _FakeTools(["get_issue"])
    mcp = mod.build_mcp_server(tools)

    async def check():
        return await mcp.call_tool("get_issue", {"arguments": {"id": "ABC-123"}})

    asyncio.run(check())
    assert tools.calls == [("get_issue", {"id": "ABC-123"})]


def test_build_mcp_server_with_no_allowed_tools_exposes_nothing(mod):
    tools = _FakeTools([])
    mcp = mod.build_mcp_server(tools)

    async def check():
        return await mcp.list_tools()

    assert asyncio.run(check()) == []


# --- main() env var validation ------------------------------------------------------------------

def test_main_requires_url_and_oauth_dir(mod, monkeypatch):
    monkeypatch.delenv("HOSTED_MCP_URL", raising=False)
    monkeypatch.delenv("HOSTED_MCP_OAUTH_DIR", raising=False)
    with pytest.raises(SystemExit, match="HOSTED_MCP_URL and HOSTED_MCP_OAUTH_DIR"):
        mod.main([])


def test_main_requires_token_endpoint_in_client_info(mod, monkeypatch, tmp_path):
    monkeypatch.setenv("HOSTED_MCP_URL", "https://mcp.example.com/mcp")
    monkeypatch.setenv("HOSTED_MCP_OAUTH_DIR", str(tmp_path))
    with pytest.raises(SystemExit, match="No token_endpoint found"):
        mod.main([])
