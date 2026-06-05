"""LiteLLM Agent Component.

Single-shot LLM agent that uses Model Context Protocol (MCP) servers as its
tool layer. Materializes one agent run per asset materialization.

The agent loop is the standard tool-calling shape:
  1. List tools from every configured MCP server (prefix tool names by
     server name so cross-server collisions are impossible).
  2. Send the system + user prompt to the LLM with all tool defs.
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
    """One MCP server connection — stdio (subprocess) or sse (HTTP)."""

    name: str = Field(
        description="Short name used to prefix tool names (e.g. 'fs' → 'fs__list_files')."
    )
    type: str = Field(
        default="stdio",
        description="Transport: 'stdio' (subprocess) or 'sse' (HTTP server-sent events).",
    )
    command: Optional[List[str]] = Field(
        default=None,
        description="stdio only: [executable, ...args]. Example: ['npx', '-y', '@modelcontextprotocol/server-filesystem', '/tmp'].",
    )
    url: Optional[str] = Field(
        default=None,
        description="sse only: full URL of the MCP SSE endpoint, e.g. 'http://localhost:3030/sse'.",
    )
    env: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra environment variables to set on the stdio subprocess.",
    )


class LiteLLMAgentComponent(Component, Model, Resolvable):
    """Single-shot LiteLLM agent with MCP tool support.

    Example:
        ```yaml
        type: dagster_component_templates.LiteLLMAgentComponent
        attributes:
          asset_name: filesystem_agent
          prompt: "Find the three largest files in /tmp and return their paths."
          system_prompt: "You are a helpful filesystem assistant."
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_iterations: 10
          mcp_servers:
            - name: fs
              type: stdio
              command:
                - npx
                - -y
                - "@modelcontextprotocol/server-filesystem"
                - /tmp
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
        description="LiteLLM model string (e.g. gpt-4o, claude-haiku-4-5-20251001, gemini/gemini-2.5-flash).",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the provider API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a custom API base URL (proxies, self-hosted).",
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
        description="List of MCP servers to expose as tools. Empty list = no tools (chat-only).",
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
        api_base_env_var = self.api_base_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _all_tags = dict(self.asset_tags or {})
        _all_tags["dagster/kind/llm"] = ""
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
                    api_base_env_var=api_base_env_var,
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


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Run the agent loop. Returns the final result dict."""
    import json
    import os
    from contextlib import AsyncExitStack

    try:
        import litellm
    except ImportError:
        raise ImportError("pip install 'litellm>=1.30.0'")

    # Connect to every MCP server. Use one AsyncExitStack so cleanup happens
    # even if any single connection fails mid-startup.
    async with AsyncExitStack() as stack:
        # tool_name (prefixed) → (session, tool_object, original_name)
        tool_index: Dict[str, Any] = {}
        # OpenAI function-calling format for litellm.completion(tools=…)
        llm_tools: List[Dict[str, Any]] = []
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
                    command=cmd[0],
                    args=list(cmd[1:]),
                    env=cfg.get("env"),
                )
                log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "sse":
                from mcp import ClientSession
                from mcp.client.sse import sse_client

                url = cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server '{name}' is sse but url is empty.")
                log.info(f"[mcp:{name}] connecting to sse: {url}")
                read, write = await stack.enter_async_context(sse_client(url))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            else:
                raise ValueError(f"MCP server '{name}' has unknown transport: {transport!r}")

            tools_response = await session.list_tools()
            for tool in tools_response.tools:
                prefixed = f"{name}__{tool.name}"
                tool_index[prefixed] = (session, tool, tool.name)
                llm_tools.append(
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

        # Build the initial message list.
        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        completion_kwargs: Dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if api_key_env_var:
            completion_kwargs["api_key"] = os.environ[api_key_env_var]
        if api_base_env_var:
            completion_kwargs["api_base"] = os.environ[api_base_env_var]

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"

        for i in range(max_iterations):
            iterations = i + 1
            kwargs = dict(completion_kwargs)
            if llm_tools:
                kwargs["tools"] = llm_tools

            response = await litellm.acompletion(messages=messages, **kwargs)
            assistant_msg = response.choices[0].message
            # Convert to dict for the next message list. LiteLLM message objects
            # have a `.model_dump()` shim on newer versions, else `.dict()`.
            try:
                assistant_dict = assistant_msg.model_dump(exclude_none=True)
            except AttributeError:
                assistant_dict = assistant_msg.dict(exclude_none=True)
            # OpenAI requires `role: assistant`
            assistant_dict.setdefault("role", "assistant")
            messages.append(assistant_dict)

            tool_calls = assistant_msg.tool_calls or []
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
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": f"Error: {err}",
                        }
                    )
                    tool_call_details.append(
                        {
                            "iteration": iterations,
                            "tool": fn_name,
                            "args": fn_args,
                            "error": err,
                        }
                    )
                    continue

                session, _tool_obj, original_name = tool_index[fn_name]
                log.info(f"[iter {iterations}] calling {fn_name} args={fn_args!r}")
                try:
                    call_result = await session.call_tool(original_name, fn_args)
                    # MCP returns a CallToolResult with .content (list of TextContent etc).
                    parts = []
                    for c in call_result.content:
                        text = getattr(c, "text", None)
                        if text is not None:
                            parts.append(text)
                        else:
                            parts.append(str(c))
                    tool_output = "\n".join(parts) if parts else "(empty)"
                    is_error = bool(getattr(call_result, "isError", False))
                except Exception as e:  # noqa: BLE001
                    tool_output = f"Tool execution error: {e}"
                    is_error = True
                    log.warning(f"tool {fn_name} raised: {e}")

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": tool_output,
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
        else:
            # for/else: hit when the loop exits via exhaustion, not break.
            stopped_reason = "max_iterations"

        final_answer = ""
        # Walk backwards to find the last assistant text message.
        for m in reversed(messages):
            if m.get("role") == "assistant" and m.get("content"):
                final_answer = m["content"]
                break

        return {
            "final_answer": final_answer,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": messages,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
        }
