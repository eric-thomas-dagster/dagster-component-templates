"""Shared MCP server registration.

MCPToolCallComponent and MCPToolPickerComponent (and this repo's litellm_agent/
openai_agent/anthropic_agent/gemini_agent/vercel_ai_gateway_agent/snowflake_cortex_agent/
agentic_pipeline components) each take an inline MCP server spec -- the same
name/type/command/url/env/headers/headers_env fields, redeclared per component. This
resource lets a project register one server ONCE and have every consuming component
reference it by `server_resource_key` instead of repeating the connection details --
and, since a Dagster resource is independently inspectable, lets a UI list "servers"
as their own thing, separate from whichever tool-call/agent assets happen to use one.

Every consuming component still accepts its own inline `server:` spec too (back-compat,
and useful for a one-off server nothing else shares) -- `server_resource_key` is an
alternative, not a replacement.
"""
import os
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


async def _open_mcp_streamable_http(stack, url: str, headers: Optional[Dict[str, str]]):
    """Enter a streamable-HTTP MCP client on `stack`; returns (read, write). Works on mcp 1.x and 2.x -- mcp 2.x
    removed `streamablehttp_client(url, headers=...)` (headers now ride on an httpx-like client passed as
    `http_client=`) and yields a 2-tuple instead of 1.x's (read, write, get_session_id). Same shim
    agentic_pipeline's own `_open_mcp_streamable_http` uses, duplicated here per this repo's self-contained-
    component convention rather than cross-importing agentic_pipeline's component.py (not on the import path
    outside a built wheel)."""
    try:
        from mcp.client.streamable_http import streamable_http_client
    except ImportError:  # older mcp 1.x
        from mcp.client.streamable_http import streamablehttp_client
        streams = await stack.enter_async_context(streamablehttp_client(url, headers=headers or None))
        return streams[0], streams[1]
    http_client = None
    if headers:
        from mcp.shared._httpx_utils import create_mcp_http_client  # httpx on 1.x, httpx2 on 2.x
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=dict(headers)))
    streams = await stack.enter_async_context(streamable_http_client(url, http_client=http_client))
    return streams[0], streams[1]


async def _list_server_tools_async(server: "MCPServerResource", timeout_seconds: float) -> List[Dict[str, Any]]:
    """Connect to `server` (stdio/http/sse) and return [{"name", "description"}, ...] -- the real, server-declared
    tool list, not a hand-maintained guess. Bounded by `timeout_seconds` (connect + handshake + list_tools)."""
    import anyio
    from contextlib import AsyncExitStack

    from mcp import ClientSession

    async def _connect_and_list():
        async with AsyncExitStack() as stack:
            if server.type == "stdio":
                from mcp import StdioServerParameters
                from mcp.client.stdio import stdio_client
                cmd = server.command or []
                if not cmd:
                    raise ValueError(f"MCP server {server.name!r} is stdio but command is empty.")
                params = StdioServerParameters(command=cmd[0], args=list(cmd[1:]), env={**os.environ, **(server.env or {})})
                read, write = await stack.enter_async_context(stdio_client(params))
            elif server.type == "http":
                url = server.url
                if not url:
                    raise ValueError(f"MCP server {server.name!r} is http but url is empty.")
                headers = dict(server.headers or {})
                for header_name, env_var in (server.headers_env or {}).items():
                    value = os.environ.get(env_var)
                    if value:
                        headers[header_name] = value
                read, write = await _open_mcp_streamable_http(stack, url, headers)
            elif server.type == "sse":
                from mcp.client.sse import sse_client
                url = server.url
                if not url:
                    raise ValueError(f"MCP server {server.name!r} is sse but url is empty.")
                headers = dict(server.headers or {})
                for header_name, env_var in (server.headers_env or {}).items():
                    value = os.environ.get(env_var)
                    if value:
                        headers[header_name] = value
                read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
            else:
                raise ValueError(f"unknown MCP server type {server.type!r}")
            session = await stack.enter_async_context(ClientSession(read, write))
            with anyio.fail_after(timeout_seconds):
                await session.initialize()
                result = await session.list_tools()
            return [{"name": t.name, "description": t.description or ""} for t in result.tools]

    return await _connect_and_list()


class MCPServerResource(dg.ConfigurableResource):
    """Same fields as MCPServerSpec/MCPPickerServerSpec (mcp_tool_call/mcp_tool_picker's own
    inline server specs) -- a consuming component reads this resource's fields into the exact
    same config shape it already builds from an inline spec, so the dispatch code downstream
    never needs to know which path it came from."""

    name: str = Field(description="Short label for logs and the mcp-server tag/metadata other components stamp on their own assets.")
    type: Literal["http", "sse", "stdio"] = Field(default="stdio", description="Transport.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL of the MCP endpoint.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars (literal values).")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: map of header_name -> env_var_name (value read from env at call time)."
    )


class MCPServerResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an MCPServerResource for use by mcp_tool_call / mcp_tool_picker / the MCP-aware
    agent components via their own `server_resource_key` field.

    Example:
        ```yaml
        type: dagster_community_components.MCPServerResourceComponent
        attributes:
          resource_key: dagster_plus_mcp
          name: dgp
          type: http
          url: https://mcp.agent.dagster.cloud/mcp/
          headers:
            Dagster-Cloud-Organization: my-org
          headers_env:
            Authorization: DAGSTER_PLUS_BEARER
        ```

        Then, in any MCP-aware component instead of its own inline `server:`:
        ```yaml
        server_resource_key: dagster_plus_mcp
        ```
    """

    resource_key: str = Field(default="mcp_server", description="Dagster resource key other components reference via server_resource_key.")
    name: str = Field(description="Short label for logs and the mcp-server tag/metadata other components stamp on their own assets.")
    type: Literal["http", "sse", "stdio"] = Field(default="stdio", description="Transport.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL of the MCP endpoint.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars (literal values).")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: map of header_name -> env_var_name (value read from env at call time)."
    )
    inspection_asset_name: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO emits an asset by this name: connects to the configured server at materialization time "
            "and lists its real tool names + descriptions as metadata (tool_names, tool_descriptions, tool_count). "
            "`allowed_tools` (on agentic_pipeline's tool_use_loop, and on the litellm/openai/anthropic/gemini/"
            "vercel_ai_gateway/snowflake_cortex agent components) matches against the NAMESPACED name "
            f"'<server name>__<tool name>' -- this asset's tool_names metadata lists the bare names; prefix each "
            "with this resource's own `name` + '__' to write an allowlist pattern. Leave unset to skip this -- "
            "existing resource-only behavior is unchanged."
        ),
    )
    inspection_timeout_seconds: float = Field(default=30.0, description="inspection_asset_name only: bounds connect + handshake + list_tools.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='MCP Server Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MCPServerResource(
            name=self.name,
            type=self.type,
            command=self.command,
            url=self.url,
            env=self.env,
            headers=self.headers,
            headers_env=self.headers_env,
        )
        defs = dg.Definitions(resources={self.resource_key: resource})
        if not self.inspection_asset_name:
            return defs

        resource_key = self.resource_key
        timeout_seconds = self.inspection_timeout_seconds
        server_name = self.name

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.inspection_asset_name),
            description=f"Lists the tools exposed by the {server_name!r} MCP server.",
            tags={"dagster-community/mcp-server": server_name.replace("/", "-")[:63]},
            required_resource_keys={resource_key},
        )
        def _mcp_server_inspection_asset(context: dg.AssetExecutionContext):
            import asyncio
            server = getattr(context.resources, resource_key)
            tools = asyncio.run(_list_server_tools_async(server, timeout_seconds))
            return dg.MaterializeResult(metadata={
                "server_name": dg.MetadataValue.text(server_name),
                "tool_count": dg.MetadataValue.int(len(tools)),
                "tool_names": dg.MetadataValue.json(sorted(t["name"] for t in tools)),
                "tool_descriptions": dg.MetadataValue.json({t["name"]: t["description"] for t in tools}),
                "transport": dg.MetadataValue.text(self.type),
            })

        return dg.Definitions.merge(defs, dg.Definitions(assets=[_mcp_server_inspection_asset]))
