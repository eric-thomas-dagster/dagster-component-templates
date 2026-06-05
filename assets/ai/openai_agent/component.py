"""OpenAI Agent Component.

Single-shot LLM agent with MCP tool support, using the OpenAI SDK directly
(no LiteLLM dependency). For multi-vendor support via LiteLLM, see
LiteLLMAgentComponent.

The agent loop is the standard tool-calling shape:
  1. List tools from every configured MCP server (prefix tool names by
     server name so cross-server collisions are impossible).
  2. Send the system + user prompt to OpenAI with all tool defs.
  3. If the model emits tool calls, dispatch each to the owning MCP
     session, append the result as a `role: tool` message, and loop.
  4. Stop when the model returns a plain text response or
     `max_iterations` is hit.

Output is a Python dict (loaded by IO manager) carrying the final answer,
full transcript, and tool-call/iteration counts.
"""
from typing import Any, Dict, List, Optional
import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


class MCPServerSpec(dg.Model, dg.Resolvable):
    """One MCP server connection — stdio (subprocess), http (streamable), or sse."""

    name: str = Field(
        description="Short name used to prefix tool names (e.g. 'fs' → 'fs__list_files')."
    )
    type: str = Field(
        default="stdio",
        description="Transport: 'stdio' (subprocess), 'http' (streamable-HTTP — modern MCP, what `claude mcp add --transport http` uses), or 'sse' (legacy server-sent events).",
    )
    command: Optional[List[str]] = Field(
        default=None,
        description="stdio only: [executable, ...args].",
    )
    url: Optional[str] = Field(
        default=None,
        description="http / sse: full URL of the MCP endpoint.",
    )
    env: Optional[Dict[str, str]] = Field(
        default=None,
        description="stdio only: extra env vars for the subprocess.",
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: literal HTTP headers (don't put secrets here — use `headers_env`).",
    )
    headers_env: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: map of header_name → env_var_name; the header value is read from the named env var at materialization time.",
    )


class OpenAIAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the OpenAI SDK directly + MCP tools.

    Example:
        ```yaml
        type: dagster_component_templates.OpenAIAgentComponent
        attributes:
          asset_name: dagster_plus_agent
          prompt: "Summarize the last 5 runs in this Dagster+ deployment."
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_iterations: 8
          mcp_servers:
            - name: dgp
              type: http
              url: https://mcp.agent.dagster.cloud/mcp/
              headers:
                Dagster-Cloud-Organization: my-org
              headers_env:
                Authorization: DAGSTER_PLUS_BEARER
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    prompt: str = Field(description="User prompt sent to the model on the first turn.")
    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt prepended to the message list.",
    )
    model_id: str = Field(
        alias="model",
        default="gpt-4o-mini",
        description="OpenAI model id (e.g. gpt-4o, gpt-4o-mini, gpt-4-turbo, o1-mini).",
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var holding the OpenAI API key.",
    )
    base_url_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding a custom base_url (Azure OpenAI / proxies / OpenAI-compatible endpoints).",
    )
    organization_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding the OpenAI organization id.",
    )
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    max_iterations: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Max tool-call rounds before bailing.",
    )
    mcp_servers: List[MCPServerSpec] = Field(
        default_factory=list,
        description="List of MCP servers to expose as tools.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        base_url_env_var = self.base_url_env_var
        organization_env_var = self.organization_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _all_tags = dict(self.asset_tags or {})
        _all_tags["dagster/kind/openai"] = ""
        _all_tags["dagster/kind/agent"] = ""

        @asset(
            name=asset_name,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _agent_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            import asyncio

            result = asyncio.run(
                _run_agent(
                    log=context.log,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    model=model,
                    api_key_env_var=api_key_env_var,
                    base_url_env_var=base_url_env_var,
                    organization_env_var=organization_env_var,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_iterations=max_iterations,
                    mcp_servers=[s.model_dump() for s in mcp_servers],
                )
            )

            md = {
                "final_answer": MetadataValue.md(result["final_answer"] or "_(empty)_"),
                "iterations": MetadataValue.int(result["iterations"]),
                "tool_calls_made": MetadataValue.int(result["tool_calls_made"]),
                "model": MetadataValue.text(model),
                "stopped_reason": MetadataValue.text(result["stopped_reason"]),
            }
            if result["tool_call_details"]:
                md["tool_calls"] = MetadataValue.json(result["tool_call_details"])
            context.add_output_metadata(md)
            return result

        return Definitions(assets=[_agent_asset])


def _resolve_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    import os

    headers: Dict[str, str] = {}
    for k, v in (cfg.get("headers") or {}).items():
        headers[k] = str(v)
    for header_name, env_var in (cfg.get("headers_env") or {}).items():
        val = os.environ.get(env_var)
        if val is None:
            raise ValueError(
                f"MCP server '{server_name}' references env var {env_var!r} for header "
                f"{header_name!r}, but it isn't set."
            )
        headers[header_name] = val
    return headers


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    base_url_env_var: Optional[str],
    organization_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    import json
    import os
    from contextlib import AsyncExitStack

    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise ImportError("pip install 'openai>=1.30.0'")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"OpenAI API key env var {api_key_env_var!r} is not set.")
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url_env_var:
        bu = os.environ.get(base_url_env_var)
        if bu:
            client_kwargs["base_url"] = bu
    if organization_env_var:
        org = os.environ.get(organization_env_var)
        if org:
            client_kwargs["organization"] = org

    client = AsyncOpenAI(**client_kwargs)

    async with AsyncExitStack() as stack:
        tool_index: Dict[str, Any] = {}
        openai_tools: List[Dict[str, Any]] = []
        servers_used: List[str] = []

        for cfg in mcp_servers:
            transport = cfg.get("type", "stdio")
            name = cfg["name"]
            if transport == "stdio":
                from mcp import ClientSession, StdioServerParameters
                from mcp.client.stdio import stdio_client

                cmd = cfg.get("command") or []
                if not cmd:
                    raise ValueError(f"MCP server '{name}' is stdio but command is empty.")
                params = StdioServerParameters(
                    command=cmd[0], args=list(cmd[1:]), env=cfg.get("env")
                )
                log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport in ("http", "streamable_http", "streamable-http"):
                from mcp import ClientSession
                from mcp.client.streamable_http import streamablehttp_client

                url = cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server '{name}' is http but url is empty.")
                headers = _resolve_headers(cfg, name)
                log.info(
                    f"[mcp:{name}] connecting via streamable-http: {url}"
                    + (f" (headers: {sorted(headers.keys())})" if headers else "")
                )
                read, write, _sid = await stack.enter_async_context(
                    streamablehttp_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "sse":
                from mcp import ClientSession
                from mcp.client.sse import sse_client

                url = cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server '{name}' is sse but url is empty.")
                headers = _resolve_headers(cfg, name)
                log.info(
                    f"[mcp:{name}] connecting to sse: {url}"
                    + (f" (headers: {sorted(headers.keys())})" if headers else "")
                )
                read, write = await stack.enter_async_context(
                    sse_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            else:
                raise ValueError(f"MCP server '{name}' has unknown transport: {transport!r}")

            tools_response = await session.list_tools()
            for tool in tools_response.tools:
                prefixed = f"{name}__{tool.name}"
                tool_index[prefixed] = (session, tool, tool.name)
                openai_tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": prefixed,
                            "description": tool.description or "",
                            "parameters": tool.inputSchema or {"type": "object", "properties": {}},
                        },
                    }
                )
            servers_used.append(name)
            log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        last_assistant_content: Optional[str] = None

        for i in range(max_iterations):
            iterations = i + 1
            kwargs: Dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if openai_tools:
                kwargs["tools"] = openai_tools

            response = await client.chat.completions.create(**kwargs)
            msg = response.choices[0].message
            try:
                msg_dict = msg.model_dump(exclude_none=True)
            except AttributeError:
                msg_dict = msg.dict(exclude_none=True)
            msg_dict.setdefault("role", "assistant")
            messages.append(msg_dict)

            if msg.content:
                last_assistant_content = msg.content

            tool_calls = msg.tool_calls or []
            if not tool_calls:
                stopped_reason = "final_answer"
                break

            for tc in tool_calls:
                fn_name = tc.function.name
                fn_args_raw = tc.function.arguments or "{}"
                try:
                    fn_args = json.loads(fn_args_raw)
                except json.JSONDecodeError as e:
                    fn_args = {}
                    log.warning(f"tool call arguments not valid JSON: {fn_args_raw!r} ({e})")

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    messages.append(
                        {"role": "tool", "tool_call_id": tc.id, "content": f"Error: {err}"}
                    )
                    tool_call_details.append(
                        {"iteration": iterations, "tool": fn_name, "args": fn_args, "error": err}
                    )
                    continue

                session, _tool_obj, original_name = tool_index[fn_name]
                log.info(f"[iter {iterations}] calling {fn_name} args={fn_args!r}")
                try:
                    call_result = await session.call_tool(original_name, fn_args)
                    parts = []
                    for c in call_result.content:
                        text = getattr(c, "text", None)
                        parts.append(text if text is not None else str(c))
                    tool_output = "\n".join(parts) if parts else "(empty)"
                    is_error = bool(getattr(call_result, "isError", False))
                except Exception as e:  # noqa: BLE001
                    tool_output = f"Tool execution error: {e}"
                    is_error = True
                    log.warning(f"tool {fn_name} raised: {e}")

                messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": tool_output}
                )
                tool_call_details.append(
                    {
                        "iteration": iterations,
                        "tool": fn_name,
                        "args": fn_args,
                        "result_preview": tool_output[:500],
                        "is_error": is_error,
                    }
                )

        return {
            "final_answer": last_assistant_content or "",
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": messages,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
        }
