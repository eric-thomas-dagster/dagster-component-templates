"""Real, committed tests for CloudflareMcpServerComponent / CloudflareMcpProxyTools.

Mocks only the unavoidable external call (`_cfo_call_tool_async`/`_cfo_list_remote_tools_async`,
the actual network hop to Cloudflare's hosted MCP server), per this repo's "mock only the paid/
external call" convention.
"""
import sys


def test_build_defs_registers_stdio_resource(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")
    component = mod.CloudflareMcpServerComponent()
    defs = component.build_defs(context=None)
    resource = defs.resources["cloudflare_mcp"]
    assert resource.type == "stdio"
    assert resource.command[0] == sys.executable
    assert "-m" in resource.command
    assert resource.env["CLOUDFLARE_MCP_API_TOKEN_ENV_VAR"] == "CLOUDFLARE_API_TOKEN"


def test_instructions_flows_from_component_into_the_registered_resource(mod):
    component = mod.CloudflareMcpServerComponent(instructions="DNS lookups only, no zone edits.")
    resource = component.build_defs(context=None).resources["cloudflare_mcp"]
    assert resource.instructions == "DNS lookups only, no zone edits."


def test_allow_list_narrows_env(mod):
    component = mod.CloudflareMcpServerComponent(allow=["search"])
    resource = component.build_defs(context=None).resources["cloudflare_mcp"]
    assert resource.env["CLOUDFLARE_MCP_ALLOW"] == "search"


# --- CloudflareMcpProxyTools ------------------------------------------------------------------

def test_missing_api_token_env_var_is_a_clean_auth_error(mod, monkeypatch):
    monkeypatch.delenv("CF_NOPE", raising=False)
    tools = mod.CloudflareMcpProxyTools("CF_NOPE")
    result = tools.call("search", {})
    assert "error" in result
    assert "CF_NOPE" in result["error"]


def test_call_returns_text_on_success(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")

    async def fake_async(api_token, tool_name, arguments, timeout_seconds=60.0):
        assert api_token == "cf_test_token"
        return f"result for {tool_name} {arguments}"

    monkeypatch.setattr(mod, "_cfo_call_tool_async", fake_async)
    tools = mod.CloudflareMcpProxyTools("CLOUDFLARE_API_TOKEN", max_requests_per_second=1000)
    result = tools.call("search", {"query": "my-zone"})
    assert "result for search" in result["text"]


def test_disallowed_tool_is_an_error_not_a_call(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")
    tools = mod.CloudflareMcpProxyTools("CLOUDFLARE_API_TOKEN", allow=["search"])
    result = tools.call("execute", {})
    assert "error" in result
    assert "not enabled" in result["error"]


def test_list_tools_filters_by_allow(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")

    async def fake_list(api_token, timeout_seconds=60.0):
        return [{"name": "search", "description": ""}, {"name": "execute", "description": ""}]

    monkeypatch.setattr(mod, "_cfo_list_remote_tools_async", fake_list)
    tools = mod.CloudflareMcpProxyTools("CLOUDFLARE_API_TOKEN", allow=["search"], max_requests_per_second=1000)
    result = tools.list_tools()
    assert [t["name"] for t in result["tools"]] == ["search"]


def test_transient_error_retries_then_succeeds(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")
    monkeypatch.setattr(mod._cfo_time, "sleep", lambda s: None)
    attempts = {"n": 0}

    async def flaky(api_token, tool_name, arguments, timeout_seconds=60.0):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("connection reset")
        return "ok"

    monkeypatch.setattr(mod, "_cfo_call_tool_async", flaky)
    tools = mod.CloudflareMcpProxyTools("CLOUDFLARE_API_TOKEN", max_requests_per_second=1000, max_retries=3)
    result = tools.call("search", {})
    assert result == {"text": "ok"}
    assert attempts["n"] == 2


def test_build_mcp_server_registers_generic_tools(mod, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf_test_token")
    tools = mod.CloudflareMcpProxyTools("CLOUDFLARE_API_TOKEN")
    mcp = mod.build_mcp_server(tools)
    assert mcp is not None
