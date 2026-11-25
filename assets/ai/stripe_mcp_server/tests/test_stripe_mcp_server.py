"""Real, committed tests for StripeMcpServerComponent / StripeMcpProxyTools.

Mocks only the unavoidable external call (`_smo_call_tool_async`/`_smo_list_remote_tools_async`,
the actual network hop to Stripe's hosted MCP server), per this repo's "mock only the paid/
external call" convention.
"""
import sys


def test_build_defs_registers_stdio_resource(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")
    component = mod.StripeMcpServerComponent()
    defs = component.build_defs(context=None)
    resource = defs.resources["stripe_mcp"]
    assert resource.type == "stdio"
    assert resource.command[0] == sys.executable
    assert "-m" in resource.command
    assert resource.env["STRIPE_MCP_API_KEY_ENV_VAR"] == "STRIPE_AGENT_API_KEY"


def test_instructions_flows_from_component_into_the_registered_resource(mod):
    component = mod.StripeMcpServerComponent(instructions="Only read-only billing lookups.")
    resource = component.build_defs(context=None).resources["stripe_mcp"]
    assert resource.instructions == "Only read-only billing lookups."


def test_allow_list_narrows_env(mod):
    component = mod.StripeMcpServerComponent(allow=["list_customers"])
    resource = component.build_defs(context=None).resources["stripe_mcp"]
    assert resource.env["STRIPE_MCP_ALLOW"] == "list_customers"


def test_stripe_account_env_var_flows_into_resource_env(mod):
    component = mod.StripeMcpServerComponent(stripe_account_env_var="MY_CONNECTED_ACCOUNT")
    resource = component.build_defs(context=None).resources["stripe_mcp"]
    assert resource.env["STRIPE_MCP_ACCOUNT_ENV_VAR"] == "MY_CONNECTED_ACCOUNT"


# --- StripeMcpProxyTools ---------------------------------------------------------------------

def test_missing_api_key_env_var_is_a_clean_auth_error(mod, monkeypatch):
    monkeypatch.delenv("STRIPE_NOPE", raising=False)
    tools = mod.StripeMcpProxyTools("STRIPE_NOPE")
    result = tools.call("list_customers", {})
    assert "error" in result
    assert "STRIPE_NOPE" in result["error"]


def test_call_returns_text_on_success(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")

    async def fake_async(api_key, stripe_account, tool_name, arguments, timeout_seconds=60.0):
        assert api_key == "rak_test_123"
        return f"result for {tool_name} {arguments}"

    monkeypatch.setattr(mod, "_smo_call_tool_async", fake_async)
    tools = mod.StripeMcpProxyTools("STRIPE_AGENT_API_KEY", max_requests_per_second=1000)
    result = tools.call("list_customers", {"limit": 5})
    assert "result for list_customers" in result["text"]


def test_disallowed_tool_is_an_error_not_a_call(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")
    tools = mod.StripeMcpProxyTools("STRIPE_AGENT_API_KEY", allow=["list_customers"])
    result = tools.call("delete_customer", {})
    assert "error" in result
    assert "not enabled" in result["error"]


def test_list_tools_filters_by_allow(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")

    async def fake_list(api_key, stripe_account, timeout_seconds=60.0):
        return [{"name": "list_customers", "description": ""}, {"name": "delete_customer", "description": ""}]

    monkeypatch.setattr(mod, "_smo_list_remote_tools_async", fake_list)
    tools = mod.StripeMcpProxyTools("STRIPE_AGENT_API_KEY", allow=["list_customers"], max_requests_per_second=1000)
    result = tools.list_tools()
    assert [t["name"] for t in result["tools"]] == ["list_customers"]


def test_transient_error_retries_then_succeeds(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")
    monkeypatch.setattr(mod._smo_time, "sleep", lambda s: None)
    attempts = {"n": 0}

    async def flaky(api_key, stripe_account, tool_name, arguments, timeout_seconds=60.0):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("connection reset")
        return "ok"

    monkeypatch.setattr(mod, "_smo_call_tool_async", flaky)
    tools = mod.StripeMcpProxyTools("STRIPE_AGENT_API_KEY", max_requests_per_second=1000, max_retries=3)
    result = tools.call("list_customers", {})
    assert result == {"text": "ok"}
    assert attempts["n"] == 2


def test_build_mcp_server_registers_generic_tools(mod, monkeypatch):
    monkeypatch.setenv("STRIPE_AGENT_API_KEY", "rak_test_123")
    tools = mod.StripeMcpProxyTools("STRIPE_AGENT_API_KEY")
    mcp = mod.build_mcp_server(tools)
    assert mcp is not None
