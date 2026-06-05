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
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'openai'].",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    # Partitions (prompt + system_prompt support {partition_key} / {partition_keys.<dim>} / {run_id}).
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'multi' | 'dynamic'.")
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static/multi.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy deadline cron schedule.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

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

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )
        _inferred_kinds = list(self.kinds or ["llm", "agent", "openai"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            _freshness_policy = (
                FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
                if self.freshness_cron
                else FreshnessPolicy.time_window(fail_window=_lag)
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            name=asset_name,
            partitions_def=partitions_def,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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
            resolved_prompt = _substitute(prompt, substitutions)
            resolved_system = _substitute(system_prompt, substitutions) if system_prompt else None

            result = asyncio.run(
                _run_agent(
                    log=context.log,
                    prompt=resolved_prompt,
                    system_prompt=resolved_system,
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


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    """Substitute `{partition_key}` / `{partition_keys.<dim>}` / `{run_id}` in a string."""
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    return out


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a partitions_def from the canonical fields. Strict combinations only."""
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type or partition_dimensions, not both."
        )

    def _axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start'")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _vals = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _vals = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _vals:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _vals or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values and partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_vals),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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
