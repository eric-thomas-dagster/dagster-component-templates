"""Real, committed tests for AtlassianMcpServerComponent / AtlassianMcpProxyTools.

Mocks only the unavoidable external call (`_amo_call_tool_async`/`_amo_list_remote_tools_async`,
the actual network hop to Atlassian's hosted MCP server), per this repo's "mock only the paid/
external call" convention.
"""
import base64
import sys


def test_build_defs_registers_stdio_resource(mod):
    component = mod.AtlassianMcpServerComponent()
    defs = component.build_defs(context=None)
    resource = defs.resources["atlassian_mcp"]
    assert resource.type == "stdio"
    assert resource.command[0] == sys.executable
    assert "-m" in resource.command
    assert resource.env["ATLASSIAN_MCP_AUTH_MODE"] == "personal_api_token"


def test_service_account_auth_mode_flows_into_resource_env(mod):
    component = mod.AtlassianMcpServerComponent(auth_mode="service_account")
    resource = component.build_defs(context=None).resources["atlassian_mcp"]
    assert resource.env["ATLASSIAN_MCP_AUTH_MODE"] == "service_account"


def test_rejects_unknown_auth_mode_at_construction():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "atlassian_mcp_server_component_tmp", Path(__file__).resolve().parent.parent / "component.py"
    )
    mod2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod2)
    import pytest

    with pytest.raises(ValueError, match="unknown auth_mode"):
        mod2.AtlassianMcpProxyTools("not_a_real_mode")


# --- personal_api_token auth header ------------------------------------------------------------

def test_personal_api_token_builds_basic_auth_header(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_EMAIL", "eric@example.com")
    monkeypatch.setenv("ATLASSIAN_API_TOKEN", "tok_123")
    tools = mod.AtlassianMcpProxyTools(
        "personal_api_token", email_env_var="ATLASSIAN_EMAIL", api_token_env_var="ATLASSIAN_API_TOKEN",
    )
    header = tools._get_auth_header()
    expected = "Basic " + base64.b64encode(b"eric@example.com:tok_123").decode("ascii")
    assert header == expected


def test_personal_api_token_missing_email_is_a_clean_auth_error(mod, monkeypatch):
    monkeypatch.delenv("ATLASSIAN_EMAIL_NOPE", raising=False)
    monkeypatch.setenv("ATLASSIAN_API_TOKEN", "tok_123")
    tools = mod.AtlassianMcpProxyTools(
        "personal_api_token", email_env_var="ATLASSIAN_EMAIL_NOPE", api_token_env_var="ATLASSIAN_API_TOKEN",
    )
    result = tools.call("jira_search", {})
    assert "error" in result
    assert "ATLASSIAN_EMAIL_NOPE" in result["error"]


# --- service_account auth header ---------------------------------------------------------------

def test_service_account_builds_bearer_auth_header(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")
    tools = mod.AtlassianMcpProxyTools("service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY")
    assert tools._get_auth_header() == "Bearer svc_key_abc"


def test_service_account_missing_key_is_a_clean_auth_error(mod, monkeypatch):
    monkeypatch.delenv("ATLASSIAN_SVC_NOPE", raising=False)
    tools = mod.AtlassianMcpProxyTools("service_account", service_account_key_env_var="ATLASSIAN_SVC_NOPE")
    result = tools.call("jira_search", {})
    assert "error" in result
    assert "ATLASSIAN_SVC_NOPE" in result["error"]


# --- call()/list_tools() ------------------------------------------------------------------------

def test_call_returns_text_on_success(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")

    async def fake_async(auth_header, tool_name, arguments, timeout_seconds=60.0):
        assert auth_header == "Bearer svc_key_abc"
        return f"result for {tool_name} {arguments}"

    monkeypatch.setattr(mod, "_amo_call_tool_async", fake_async)
    tools = mod.AtlassianMcpProxyTools(
        "service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY", max_requests_per_second=1000,
    )
    result = tools.call("jira_search", {"jql": "project = ENG"})
    assert "result for jira_search" in result["text"]


def test_disallowed_tool_is_an_error_not_a_call(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")
    tools = mod.AtlassianMcpProxyTools(
        "service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY", allow=["jira_search"],
    )
    result = tools.call("delete_issue", {})
    assert "error" in result
    assert "not enabled" in result["error"]


def test_list_tools_filters_by_allow(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")

    async def fake_list(auth_header, timeout_seconds=60.0):
        return [{"name": "jira_search", "description": ""}, {"name": "delete_issue", "description": ""}]

    monkeypatch.setattr(mod, "_amo_list_remote_tools_async", fake_list)
    tools = mod.AtlassianMcpProxyTools(
        "service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY", allow=["jira_search"], max_requests_per_second=1000,
    )
    result = tools.list_tools()
    assert [t["name"] for t in result["tools"]] == ["jira_search"]


def test_transient_error_retries_then_succeeds(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")
    monkeypatch.setattr(mod._amo_time, "sleep", lambda s: None)
    attempts = {"n": 0}

    async def flaky(auth_header, tool_name, arguments, timeout_seconds=60.0):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("connection reset")
        return "ok"

    monkeypatch.setattr(mod, "_amo_call_tool_async", flaky)
    tools = mod.AtlassianMcpProxyTools(
        "service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY", max_requests_per_second=1000, max_retries=3,
    )
    result = tools.call("jira_search", {})
    assert result == {"text": "ok"}
    assert attempts["n"] == 2


def test_build_mcp_server_registers_generic_tools(mod, monkeypatch):
    monkeypatch.setenv("ATLASSIAN_SVC_KEY", "svc_key_abc")
    tools = mod.AtlassianMcpProxyTools("service_account", service_account_key_env_var="ATLASSIAN_SVC_KEY")
    mcp = mod.build_mcp_server(tools)
    assert mcp is not None


def test_stdio_server_missing_auth_is_a_real_isError_result_with_the_original_message():
    """Full round trip through a real subprocess (same transport agentic_pipeline/slack_chat_tools etc. use)
    -- not just calling the registered function in-process -- since the SDK's tool dispatch only preserves a
    raised exception's own message when it recognizes the exception as ITS OWN ToolError (see _mcp_call in
    component.py); getting this wrong (e.g. raising this module's own ToolError instead) still produces
    isError=True but replaces the message with a useless generic 'Error executing tool <name>', mirroring
    dagster_instance_tools/tests/test_mcp_stdio.py's identical check. Uses a deliberately-unset auth env var
    (not a network mock) as the real, known failure -- AtlassianMcpProxyTools._get_auth_header raises
    AtlassianMcpAuthError -> tools.call() returns {"error": ...} -> _mcp_call must turn that into isError."""
    import asyncio
    import os
    import sys
    from pathlib import Path

    import pytest

    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    server = str(Path(__file__).resolve().parent.parent / "component.py")
    unset_email_var = "ATLASSIAN_EMAIL_TEST_UNSET_VAR"
    unset_token_var = "ATLASSIAN_API_TOKEN_TEST_UNSET_VAR"
    env = {k: v for k, v in os.environ.items() if k not in (unset_email_var, unset_token_var)}
    env.update({
        "ATLASSIAN_MCP_AUTH_MODE": "personal_api_token",
        "ATLASSIAN_MCP_EMAIL_ENV_VAR": unset_email_var,
        "ATLASSIAN_MCP_API_TOKEN_ENV_VAR": unset_token_var,
    })
    params = StdioServerParameters(command=sys.executable, args=[server], env=env)

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.call_tool("call_remote_tool", {"tool_name": "jira_search", "arguments": {}})

    result = asyncio.run(asyncio.wait_for(go(), 60))
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    text = result.content[0].text
    assert unset_email_var in text and "requires" in text
