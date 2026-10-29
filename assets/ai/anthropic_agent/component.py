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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class AnthropicAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the Anthropic SDK directly + MCP tools.

    Example:
        ```yaml
        type: dagster_community_components.AnthropicAgentComponent
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
    thinking_budget: Optional[int] = Field(
        default=None,
        description=(
            "Max reasoning-trace tokens for Claude thinking mode "
            "(claude-opus-4.1+, claude-sonnet-4+). When set, Anthropic returns "
            "a `thinking` block alongside the visible answer showing the "
            "model's reasoning trace. Cost implication: thinking tokens are "
            "billed as output tokens. Leave None (default) for standard "
            "non-thinking behavior."
        ),
    )
    prompt_caching: bool = Field(
        default=False,
        description=(
            "Anthropic prompt caching. When true, wraps the system prompt "
            "with `cache_control: {type: ephemeral}`. First call warms "
            "the cache (~25% surcharge on that call); subsequent calls "
            "within ~5 min read from cache (~90% cheaper on the cached "
            "prefix). Big win for long system prompts + MCP tool schemas "
            "that repeat across pipeline runs OR across loop iterations."
        ),
    )
    max_iterations: int = Field(default=10, ge=1, le=100, description="Max tool-call rounds.")
    mcp_servers: List[MCPServerSpec] = Field(default_factory=list, description="MCP servers to expose as tools.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'anthropic'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'multi' | 'dynamic'.")

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static/multi.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy deadline cron schedule.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        base_url_env_var = self.base_url_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        thinking_budget = self.thinking_budget
        prompt_caching = self.prompt_caching
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        _inferred_kinds = list(self.kinds or ["llm", "agent", "anthropic"])
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
            key=AssetKey.from_user_string(asset_name),
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
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_iterations=max_iterations,
                    mcp_servers=[s.model_dump() for s in mcp_servers],
                    thinking_budget=thinking_budget,
                    prompt_caching=prompt_caching,
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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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

            url = cfg.get("url")
            if not url:
                raise ValueError(f"MCP server '{name}' is http but url is empty.")
            headers = _resolve_headers(cfg, name)
            log.info(
                f"[mcp:{name}] connecting via streamable-http: {url}"
                + (f" (headers: {sorted(headers.keys())})" if headers else "")
            )
            read, write = await _open_mcp_streamable_http(stack, url, headers)
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
                    "input_schema": _mcp_field(tool, "inputSchema", None) or {"type": "object", "properties": {}},
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
    thinking_budget: Optional[int] = None,
    prompt_caching: bool = False,
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
                # Anthropic prompt caching lives on the system field. When on,
                # convert the string form to a content-block list with a
                # cache_control marker. The Anthropic SDK translates it to the
                # `anthropic-beta: prompt-caching-2024-07-31` request header
                # transparently.
                if prompt_caching:
                    kwargs["system"] = [
                        {"type": "text", "text": system_prompt,
                         "cache_control": {"type": "ephemeral"}}
                    ]
                else:
                    kwargs["system"] = system_prompt
            if anthropic_tools:
                kwargs["tools"] = anthropic_tools
            # Claude thinking mode: enabled per-request. thinking tokens are
            # billed as output tokens and appear as a `thinking` block in the
            # response alongside the visible `text` block.
            if thinking_budget is not None:
                kwargs["thinking"] = {
                    "type": "enabled",
                    "budget_tokens": int(thinking_budget),
                }

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
                    parts, is_error = _mcp_result_parts(call_result)
                    tool_output = "\n".join(parts) if parts else "(empty)"
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


# --- mcp-compat-helpers:begin (identical in every MCP-using component; kept in sync by tests/test_mcp_compat_helpers_drift.py) ---
def _mcp_field(obj, key, default=None):
    """Read `key` (camelCase) from an MCP SDK object or dict. mcp 2.x renamed model attributes to snake_case
    (`inputSchema`->`input_schema`, `isError`->`is_error`, `structuredContent`->`structured_content`) and the models
    have no dict-style `.get`; fastmcp-client variants may return plain dicts."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    import re
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
    for k in (key, snake):  # mcp 1.x models expose camelCase (inputSchema/isError); mcp 2.x snake_case (input_schema/is_error)
        val = getattr(obj, k, None)
        if val is not None:
            return val
    return default


async def _open_mcp_streamable_http(stack, url, headers):
    """Enter a streamable-HTTP MCP client on `stack`; returns (read, write). Works on mcp 1.x and 2.x.

    mcp 2.x removed `streamablehttp_client(url, headers=...)` (headers now ride on an `httpx2.AsyncClient` passed as
    `http_client=`) and yields a 2-tuple instead of 1.x's (read, write, get_session_id). mcp >=1.24 also has
    `streamable_http_client(url, http_client=...)`; older 1.x only has the deprecated `streamablehttp_client`."""
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


def _mcp_result_parts(call_result):
    """CallToolResult -> (list of text parts, is_error). Falls back to structured content when a tool returned only that
    (mcp 2.x / fastmcp); reads `isError`/`is_error` on either major."""
    parts = []
    for c in _mcp_field(call_result, "content", None) or []:
        text = getattr(c, "text", None)
        parts.append(text if text is not None else str(c))
    if not parts:
        structured = _mcp_field(call_result, "structuredContent", None)
        if structured is not None:
            import json
            parts.append(json.dumps(structured, default=str))
    return parts, bool(_mcp_field(call_result, "isError", False))
# --- mcp-compat-helpers:end ---
