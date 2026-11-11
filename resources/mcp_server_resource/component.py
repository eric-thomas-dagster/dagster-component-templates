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
from typing import Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


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
        return dg.Definitions(resources={self.resource_key: resource})
