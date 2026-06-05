"""Gemini Agent Component.

Single-shot LLM agent with MCP tool support, using the Google Gen AI SDK
directly (no LiteLLM dependency). For multi-vendor support, see
LiteLLMAgentComponent.

Gemini's tool-calling shape differs from OpenAI/Anthropic:
  - Tools are `FunctionDeclaration`s wrapped in `Tool` objects.
  - Request: `client.aio.models.generate_content(model, contents, config)`.
  - Response: `candidates[0].content.parts` — each part has either `text` or
    `function_call`.
  - Tool results go back as a `user` Content with `Part.from_function_response`
    blocks.

`thinking_budget` defaults to 0 because thinking tokens come out of
`max_output_tokens` on Gemini 2.5+ and silently truncate short outputs.
For long multi-step plans you may want to raise it; see Field doc.

Output: same dict shape as the other *_agent components.
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
        default=None, description="http / sse: header_name → env_var_name, value read from env."
    )


class GeminiAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the Google Gen AI SDK + MCP tools.

    Example:
        ```yaml
        type: dagster_component_templates.GeminiAgentComponent
        attributes:
          asset_name: dagster_plus_gemini_agent
          prompt: "Summarize the last 5 runs in this Dagster+ deployment."
          model: gemini-2.5-flash
          api_key_env_var: GEMINI_API_KEY
          max_iterations: 8
          # Gemini-2.5: thinking tokens eat into max_output_tokens.
          # Default 0 (no thinking) is safest for short structured outputs.
          thinking_budget: 0
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
    system_prompt: Optional[str] = Field(default=None, description="System instruction.")
    model_id: str = Field(
        alias="model",
        default="gemini-2.5-flash",
        description="Gemini model id (e.g. gemini-2.5-pro, gemini-2.5-flash, gemini-2.5-flash-lite).",
    )
    api_key_env_var: str = Field(default="GEMINI_API_KEY", description="Env var with the Gemini API key.")
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_output_tokens: int = Field(default=2048, description="Max output tokens per model call.")
    thinking_budget: Optional[int] = Field(
        default=0,
        description="Gemini 2.5+ thinking budget. Defaults to 0 — thinking tokens come out of max_output_tokens and can silently truncate short outputs. Raise (e.g. 1024) only if your prompt needs multi-step reasoning beyond what the iteration loop already provides.",
    )
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
        temperature = self.temperature
        max_output_tokens = self.max_output_tokens
        thinking_budget = self.thinking_budget
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _all_tags = dict(self.asset_tags or {})
        _all_tags["dagster/kind/gemini"] = ""
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
                    temperature=temperature,
                    max_output_tokens=max_output_tokens,
                    thinking_budget=thinking_budget,
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
    """Bring up every MCP server. Returns (tool_index, gemini_tools, servers_used).

    `gemini_tools` is a list with one `types.Tool` carrying all FunctionDeclarations.
    """
    from google.genai import types as gtypes

    tool_index: Dict[str, Any] = {}
    function_decls: List[Any] = []
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
            # Gemini accepts the JSONSchema input_schema via parameters_json_schema.
            function_decls.append(
                gtypes.FunctionDeclaration(
                    name=prefixed,
                    description=tool.description or "",
                    parameters_json_schema=tool.inputSchema or {"type": "object", "properties": {}},
                )
            )
        servers_used.append(name)
        log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

    gemini_tools = [gtypes.Tool(function_declarations=function_decls)] if function_decls else []
    return tool_index, gemini_tools, servers_used


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    temperature: float,
    max_output_tokens: int,
    thinking_budget: Optional[int],
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    import os
    from contextlib import AsyncExitStack

    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError:
        raise ImportError("pip install 'google-genai>=0.7.0'")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"Gemini API key env var {api_key_env_var!r} is not set.")
    client = genai.Client(api_key=api_key)

    async with AsyncExitStack() as stack:
        tool_index, gemini_tools, servers_used = await _connect_mcp(stack, log, mcp_servers)

        # Build the running `contents` list.
        contents: List[Any] = [
            gtypes.Content(role="user", parts=[gtypes.Part.from_text(text=prompt)])
        ]

        cfg_kwargs: Dict[str, Any] = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        if system_prompt:
            cfg_kwargs["system_instruction"] = system_prompt
        if gemini_tools:
            cfg_kwargs["tools"] = gemini_tools
        if thinking_budget is not None:
            cfg_kwargs["thinking_config"] = gtypes.ThinkingConfig(
                thinking_budget=int(thinking_budget)
            )

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        last_text_answer = ""

        for i in range(max_iterations):
            iterations = i + 1
            response = await client.aio.models.generate_content(
                model=model,
                contents=contents,
                config=gtypes.GenerateContentConfig(**cfg_kwargs),
            )

            candidate = response.candidates[0] if response.candidates else None
            if candidate is None:
                stopped_reason = "no_candidate"
                break

            # Walk parts: collect text + function_calls. Append the assistant
            # turn back to contents so the next iteration sees it.
            text_parts = []
            function_calls = []
            for part in candidate.content.parts or []:
                if getattr(part, "function_call", None):
                    function_calls.append(part.function_call)
                if getattr(part, "text", None):
                    text_parts.append(part.text)

            if text_parts:
                last_text_answer = "\n".join(t for t in text_parts if t)
            contents.append(candidate.content)

            if not function_calls:
                stopped_reason = "final_answer"
                break

            # Dispatch every function call and reply with all function_responses
            # in a single user-role Content (Gemini convention).
            response_parts = []
            for fc in function_calls:
                fn_name = fc.name
                fn_args = dict(fc.args or {})

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    response_parts.append(
                        gtypes.Part.from_function_response(
                            name=fn_name, response={"error": err}
                        )
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

                response_parts.append(
                    gtypes.Part.from_function_response(
                        name=fn_name,
                        response={"output": tool_output, "is_error": is_error},
                    )
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

            contents.append(gtypes.Content(role="user", parts=response_parts))

        # Build a serializable transcript (strip non-pickleable SDK objects).
        transcript_serial = []
        for c in contents:
            try:
                transcript_serial.append(c.model_dump(exclude_none=True))
            except Exception:
                transcript_serial.append(str(c))

        return {
            "final_answer": last_text_answer,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": transcript_serial,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
        }
