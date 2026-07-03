"""Vercel AI Gateway Agent Component.

Single-shot LLM agent using Vercel's AI Gateway — an OpenAI-compatible
proxy that routes to any supported provider (OpenAI, Anthropic, Google,
xAI, Groq, etc.) with a single Vercel API token, unified billing,
automatic fallback, and Vercel-side observability.

Endpoint: https://ai-gateway.vercel.sh/v1/chat/completions
Model strings use ``<provider>/<model>`` format:
  - openai/gpt-4o, openai/gpt-4o-mini
  - anthropic/claude-sonnet-4-6, anthropic/claude-haiku-4-5
  - google/gemini-2.5-pro, google/gemini-2.5-flash
  - xai/grok-4, xai/grok-3-mini
  - groq/llama-3.3-70b, groq/mixtral-8x7b
  - (see https://vercel.com/ai-gateway/models for current list)

Because the endpoint is OpenAI-compatible, we reuse the openai Python SDK
with a custom base_url. This component is the "prefer the Vercel gateway"
version of ``openai_agent`` — same shape, different base URL, cheaper
if you have Vercel AI credits, one bill.

Supports MCP tool calls with the same shape as ``openai_agent`` /
``anthropic_agent`` (via the openai SDK's tool-calling API).

Auth: Vercel API token via env var (default ``VERCEL_API_TOKEN``). The
gateway automatically routes to the correct upstream provider and bills
against your Vercel account.

Docs: https://vercel.com/docs/ai-gateway
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
    """MCP server spec (mirrors openai_agent / anthropic_agent shape)."""

    name: str = Field(description="Short name used to prefix tool names.")
    type: str = Field(default="stdio", description="Transport: 'stdio', 'http', or 'sse'.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: header_name → env_var_name (value read from env at runtime).",
    )


class VercelAIGatewayAgentComponent(Component, Model, Resolvable):
    """Single-shot LLM agent via Vercel AI Gateway.

    OpenAI-compatible: swap ``model`` strings to route to any provider Vercel
    supports without changing your API key or endpoint. Supports MCP tool calls.

    Example:
        ```yaml
        type: dagster_community_components.VercelAIGatewayAgentComponent
        attributes:
          asset_name: research_agent
          prompt: "Summarize the latest Dagster+ release notes."
          model: anthropic/claude-sonnet-4-6
          api_key_env_var: VERCEL_API_TOKEN
          max_iterations: 6
          mcp_servers:
            - name: dgp
              type: http
              url: https://mcp.agent.dagster.cloud/mcp/
              headers_env:
                Authorization: DAGSTER_PLUS_BEARER
        ```

    Fallback example — route to Claude, fall back to OpenAI on failure:
        ```yaml
        attributes:
          model: anthropic/claude-sonnet-4-6
          fallback_models:
            - openai/gpt-4o
            - google/gemini-2.5-flash
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    prompt: str = Field(description="User prompt sent on the first turn.")
    system_prompt: Optional[str] = Field(default=None, description="System prompt.")

    model_id: str = Field(
        alias="model",
        default="openai/gpt-4o-mini",
        description=(
            "Vercel AI Gateway model string, format '<provider>/<model>'. "
            "Examples: openai/gpt-4o-mini, anthropic/claude-sonnet-4-6, "
            "google/gemini-2.5-flash, xai/grok-4, groq/llama-3.3-70b."
        ),
    )
    fallback_models: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional fallback model strings. On invocation error (rate limit, "
            "provider outage), retry with each in order."
        ),
    )
    api_key_env_var: str = Field(
        default="VERCEL_API_TOKEN",
        description="Env var with the Vercel API token (create at https://vercel.com/account/tokens).",
    )
    api_base_url: str = Field(
        default="https://ai-gateway.vercel.sh/v1",
        description="Vercel AI Gateway base URL (rarely overridden).",
    )
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    max_iterations: int = Field(default=10, ge=1, le=100, description="Max tool-call rounds.")
    mcp_servers: List[MCPServerSpec] = Field(
        default_factory=list,
        description="MCP servers to expose as tools.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['llm', 'agent', 'vercel', 'ai-gateway'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        _self = self
        _kinds = list(self.kinds or ["llm", "agent", "vercel", "ai-gateway"])
        _all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            _all_tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            description=_self.description or (
                f"Vercel AI Gateway agent — routes through gateway to {_self.model_id!r}. "
                f"Falls back to {_self.fallback_models}" if _self.fallback_models else
                f"Vercel AI Gateway agent → {_self.model_id!r}"
            ),
            owners=_self.owners or [],
            tags=_all_tags,
            deps=[AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _agent_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            import asyncio

            substitutions = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = {}
            resolved_prompt = _substitute(_self.prompt, substitutions)
            resolved_system = _substitute(_self.system_prompt, substitutions) if _self.system_prompt else None

            models_to_try = [_self.model_id] + list(_self.fallback_models or [])
            last_error: Optional[Exception] = None
            for m in models_to_try:
                try:
                    result = asyncio.run(
                        _run_agent(
                            log=context.log,
                            prompt=resolved_prompt,
                            system_prompt=resolved_system,
                            model=m,
                            api_key_env_var=_self.api_key_env_var,
                            api_base_url=_self.api_base_url,
                            temperature=_self.temperature,
                            max_tokens=_self.max_tokens,
                            max_iterations=_self.max_iterations,
                            mcp_servers=[s.model_dump() for s in _self.mcp_servers],
                        )
                    )
                    result["model_used"] = m
                    result["fallback_chain"] = models_to_try
                    break
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    context.log.warning(
                        f"[vercel_ai_gateway] model {m!r} failed: {e}. "
                        f"Trying next in fallback chain."
                    )
            else:
                raise RuntimeError(
                    f"Every model in fallback chain {models_to_try} failed. Last error: {last_error}"
                )

            md = {
                "final_answer": MetadataValue.md(result["final_answer"] or "_(empty)_"),
                "iterations": MetadataValue.int(result["iterations"]),
                "tool_calls_made": MetadataValue.int(result["tool_calls_made"]),
                "model_requested": MetadataValue.text(_self.model_id),
                "model_used": MetadataValue.text(result["model_used"]),
                "gateway": MetadataValue.text("vercel-ai-gateway"),
                "stopped_reason": MetadataValue.text(result["stopped_reason"]),
            }
            if result["tool_call_details"]:
                md["tool_calls"] = MetadataValue.json(result["tool_call_details"])
            if len(models_to_try) > 1:
                md["fallback_chain"] = MetadataValue.json(models_to_try)
            context.add_output_metadata(md)
            return result

        return Definitions(assets=[_agent_asset])


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    if not s or "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    return out


async def _connect_mcp(stack, log, mcp_servers):
    """Bring up every MCP server. Returns (tool_index, openai_tools, servers_used)."""
    import os

    tool_index: Dict[str, Any] = {}
    openai_tools: List[Dict[str, Any]] = []
    servers_used: List[str] = []

    for cfg in mcp_servers:
        transport = cfg.get("type", "stdio")
        name = cfg["name"]

        headers: Dict[str, str] = {}
        for k, v in (cfg.get("headers") or {}).items():
            headers[k] = str(v)
        for header_name, env_var in (cfg.get("headers_env") or {}).items():
            val = os.environ.get(env_var)
            if val is None:
                raise ValueError(
                    f"MCP server '{name}' references env var {env_var!r} for header "
                    f"{header_name!r}, but it isn't set."
                )
            headers[header_name] = val

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
            log.info(f"[mcp:{name}] connecting via streamable-http: {url}")
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
            log.info(f"[mcp:{name}] connecting to sse: {url}")
            read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
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

    return tool_index, openai_tools, servers_used


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    api_base_url: str,
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    import os
    import json
    from contextlib import AsyncExitStack

    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise ImportError("pip install 'openai>=1.0.0' — the Vercel AI Gateway is OpenAI-compatible")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"Vercel API token env var {api_key_env_var!r} is not set.")

    client = AsyncOpenAI(api_key=api_key, base_url=api_base_url)

    async with AsyncExitStack() as stack:
        tool_index, openai_tools, servers_used = await _connect_mcp(stack, log, mcp_servers)

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        last_text_answer = ""

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
            assistant_msg: Dict[str, Any] = {"role": "assistant"}
            if msg.content:
                assistant_msg["content"] = msg.content
                last_text_answer = msg.content
            if getattr(msg, "tool_calls", None):
                assistant_msg["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in msg.tool_calls
                ]
            messages.append(assistant_msg)

            finish_reason = response.choices[0].finish_reason
            if finish_reason != "tool_calls" or not getattr(msg, "tool_calls", None):
                stopped_reason = "final_answer"
                break

            # Dispatch each tool call.
            for tc in msg.tool_calls:
                fn_name = tc.function.name
                try:
                    fn_args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                except json.JSONDecodeError:
                    fn_args = {}

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": f"Error: {err}",
                    })
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

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_output,
                })
                tool_call_details.append({
                    "iteration": iterations,
                    "tool": fn_name,
                    "args": fn_args,
                    "result_preview": tool_output[:500],
                    "is_error": is_error,
                })

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
