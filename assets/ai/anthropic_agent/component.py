"""Anthropic Agent Component.

Single-shot LLM agent with MCP tool support, using the Anthropic SDK directly
(no LiteLLM dependency). For multi-vendor support, see LiteLLMAgentComponent.

Anthropic's tool-calling shape differs from OpenAI's:
  - Tools: {name, description, input_schema} (no nested `function` wrapper)
  - Response: a list of content blocks, some `text`, some `tool_use`
  - Tool results: posted as `role: user` messages with `tool_result` content
    blocks (NOT a separate `role: tool`)

Output: a dict with the same shape as the other *_agent components for
consistency: {final_answer, iterations, tool_calls_made, tool_call_details,
transcript, stopped_reason, model, mcp_servers}.
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
    name: str = Field(description="Short name used to prefix tool names.")
    type: str = Field(
        default="stdio",
        description="Transport: 'stdio' (subprocess), 'http' (streamable-HTTP), or 'sse' (legacy).",
    )
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: header_name → env_var_name, value read from env at materialization."
    )


class AnthropicAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the Anthropic SDK directly + MCP tools.

    Example:
        ```yaml
        type: dagster_component_templates.AnthropicAgentComponent
        attributes:
          asset_name: dagster_plus_claude_agent
          prompt: "Summarize the last 5 runs in this Dagster+ deployment."
          model: claude-haiku-4-5-20251001
          api_key_env_var: ANTHROPIC_API_KEY
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
    system_prompt: Optional[str] = Field(default=None, description="System prompt (Anthropic top-level param).")
    model_id: str = Field(
        alias="model",
        default="claude-haiku-4-5-20251001",
        description="Anthropic model id (e.g. claude-opus-4-7, claude-sonnet-4-6, claude-haiku-4-5-20251001).",
    )
    api_key_env_var: str = Field(default="ANTHROPIC_API_KEY", description="Env var with the Anthropic API key.")
    base_url_env_var: Optional[str] = Field(default=None, description="Optional env var with a custom base_url.")
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    max_iterations: int = Field(default=10, ge=1, le=100, description="Max tool-call rounds.")
    mcp_servers: List[MCPServerSpec] = Field(default_factory=list, description="MCP servers to expose as tools.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        base_url_env_var = self.base_url_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _all_tags = dict(self.asset_tags or {})
        _all_tags["dagster/kind/anthropic"] = ""
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


async def _connect_mcp(stack, log, mcp_servers):
    """Bring up every MCP server. Returns (tool_index, anthropic_tools, servers_used)."""
    tool_index: Dict[str, Any] = {}
    anthropic_tools: List[Dict[str, Any]] = []
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
            params = StdioServerParameters(command=cmd[0], args=list(cmd[1:]), env=cfg.get("env"))
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
            read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        else:
            raise ValueError(f"MCP server '{name}' has unknown transport: {transport!r}")

        tools_response = await session.list_tools()
        for tool in tools_response.tools:
            prefixed = f"{name}__{tool.name}"
            tool_index[prefixed] = (session, tool, tool.name)
            anthropic_tools.append(
                {
                    "name": prefixed,
                    "description": tool.description or "",
                    "input_schema": tool.inputSchema or {"type": "object", "properties": {}},
                }
            )
        servers_used.append(name)
        log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

    return tool_index, anthropic_tools, servers_used


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    base_url_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    import os
    from contextlib import AsyncExitStack

    try:
        from anthropic import AsyncAnthropic
    except ImportError:
        raise ImportError("pip install 'anthropic>=0.40.0'")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"Anthropic API key env var {api_key_env_var!r} is not set.")
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url_env_var:
        bu = os.environ.get(base_url_env_var)
        if bu:
            client_kwargs["base_url"] = bu
    client = AsyncAnthropic(**client_kwargs)

    async with AsyncExitStack() as stack:
        tool_index, anthropic_tools, servers_used = await _connect_mcp(stack, log, mcp_servers)

        messages: List[Dict[str, Any]] = [{"role": "user", "content": prompt}]

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        last_text_answer = ""

        for i in range(max_iterations):
            iterations = i + 1
            kwargs: Dict[str, Any] = {
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
            if system_prompt:
                kwargs["system"] = system_prompt
            if anthropic_tools:
                kwargs["tools"] = anthropic_tools

            response = await client.messages.create(**kwargs)
            # Append the assistant's full content list back into messages.
            assistant_content = []
            tool_uses = []
            text_parts = []
            for block in response.content:
                btype = getattr(block, "type", None)
                if btype == "text":
                    text_parts.append(block.text)
                    assistant_content.append({"type": "text", "text": block.text})
                elif btype == "tool_use":
                    tool_uses.append(block)
                    assistant_content.append(
                        {
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        }
                    )

            if text_parts:
                last_text_answer = "\n".join(text_parts)

            messages.append({"role": "assistant", "content": assistant_content})

            stop_reason = getattr(response, "stop_reason", None)
            if stop_reason != "tool_use" or not tool_uses:
                stopped_reason = "final_answer"
                break

            # Dispatch every tool_use block in this turn and reply with one
            # `user` message that bundles all the `tool_result` blocks.
            tool_results_content = []
            for tu in tool_uses:
                fn_name = tu.name
                fn_args = tu.input or {}

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    tool_results_content.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tu.id,
                            "content": f"Error: {err}",
                            "is_error": True,
                        }
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

                tool_results_content.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": tool_output,
                        "is_error": is_error,
                    }
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

            messages.append({"role": "user", "content": tool_results_content})

        return {
            "final_answer": last_text_answer,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": messages,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
        }
