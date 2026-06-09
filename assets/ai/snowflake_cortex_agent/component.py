"""Snowflake Cortex Agent Component.

Single-shot LLM agent backed by Snowflake Cortex COMPLETE — the
`/api/v2/cortex/inference:complete` REST endpoint. Same MCP tool-calling
loop as `openai_agent` / `anthropic_agent`; the LLM call is just routed
through Snowflake's REST API instead of a vendor SDK.

Why this exists: customers already paying for Snowflake credit can run
their agent loops *inside* their Snowflake account — same governance,
no separate model vendor key, models served from the same warehouse
your data sits on. Pairs naturally with the existing
`snowflake_cortex_asset` (per-row Cortex SQL) and `snowflake_cortex_search`
(managed RAG) — those are the source / retrieval shape; this is the
agent-loop shape.

Auth: a Snowflake PAT (personal access token) or OAuth bearer token,
read from `auth_token_env_var`. Endpoint base URL is
`https://<account>.snowflakecomputing.com` from `account_url_env_var`.

Note: code-validated only — needs a Snowflake account with Cortex enabled
to live-validate against. The HTTP+JSON shape follows Snowflake's public
Cortex Inference API docs.
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
    """Same MCPServerSpec shape as the other *_agent components."""
    name: str = Field(description="Tool-name prefix.")
    type: str = Field(default="stdio", description="'stdio' | 'http' | 'sse'.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: header_name → env_var_name."
    )


class SnowflakeCortexAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using Snowflake Cortex COMPLETE + MCP tools.

    Example:
        ```yaml
        type: dagster_component_templates.SnowflakeCortexAgentComponent
        attributes:
          asset_name: dagster_plus_cortex_agent
          prompt: "Summarize the last 5 runs."
          model: claude-3-5-sonnet
          account_url_env_var: SNOWFLAKE_ACCOUNT_URL
          auth_token_env_var: SNOWFLAKE_PAT
          max_iterations: 6
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
    system_prompt: Optional[str] = Field(default=None, description="System prompt prepended.")
    model_id: str = Field(
        alias="model",
        default="claude-3-5-sonnet",
        description="Cortex model id (claude-3-5-sonnet, llama3.1-70b, mistral-large2, etc.). See `SNOWFLAKE.CORTEX.COMPLETE` docs for the current list.",
    )
    account_url_env_var: str = Field(
        default="SNOWFLAKE_ACCOUNT_URL",
        description="Env var with `https://<account>.snowflakecomputing.com`.",
    )
    auth_token_env_var: str = Field(
        default="SNOWFLAKE_PAT",
        description="Env var with the Snowflake PAT / OAuth bearer token used in the Authorization header.",
    )
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    max_iterations: int = Field(default=10, ge=1, le=100, description="Max tool-call rounds.")
    mcp_servers: List[MCPServerSpec] = Field(default_factory=list, description="MCP servers to expose as tools.")
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['llm', 'agent', 'snowflake', 'cortex'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only deps.")

    # Partitions (same shape as the other agents).
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'multi' | 'dynamic'.")
    partition_start: Optional[str] = Field(default=None, description="ISO date.")
    partition_values: Optional[str] = Field(default=None, description="CSV.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Dynamic partition name.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis spec.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Delay between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' | 'exponential'.")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        model = self.model_id
        account_url_env_var = self.account_url_env_var
        auth_token_env_var = self.auth_token_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        request_timeout_seconds = self.request_timeout_seconds
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        _inferred_kinds = list(self.kinds or ["llm", "agent", "snowflake", "cortex"])
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
                    account_url_env_var=account_url_env_var,
                    auth_token_env_var=auth_token_env_var,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_iterations=max_iterations,
                    request_timeout_seconds=request_timeout_seconds,
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
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start'")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            return DynamicPartitionsDefinition(name=spec.get("dynamic_partition_name") or spec.get("name"))
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
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _vals: raise ValueError("static requires values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name: raise ValueError("dynamic requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _vals or not partition_start: raise ValueError("multi requires partition_values + partition_start.")
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
            raise ValueError(f"server {server_name!r}: env var {env_var!r} for header {header_name!r} is not set.")
        headers[header_name] = val
    return headers


async def _connect_mcp(stack, log, mcp_servers):
    """Same MCP connect + tool discovery as the other agents."""
    tool_index: Dict[str, Any] = {}
    cortex_tools: List[Dict[str, Any]] = []
    servers_used: List[str] = []

    for cfg in mcp_servers:
        transport = cfg.get("type", "stdio")
        name = cfg["name"]
        if transport == "stdio":
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
            cmd = cfg.get("command") or []
            if not cmd:
                raise ValueError(f"server {name!r} stdio without command")
            params = StdioServerParameters(command=cmd[0], args=list(cmd[1:]), env=cfg.get("env"))
            log.info(f"[mcp:{name}] starting stdio: {' '.join(cmd)}")
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport in ("http", "streamable_http", "streamable-http"):
            from mcp import ClientSession
            from mcp.client.streamable_http import streamablehttp_client
            url = cfg.get("url")
            if not url:
                raise ValueError(f"server {name!r} http without url")
            headers = _resolve_headers(cfg, name)
            log.info(f"[mcp:{name}] connecting http: {url}")
            read, write, _ = await stack.enter_async_context(streamablehttp_client(url, headers=headers or None))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "sse":
            from mcp import ClientSession
            from mcp.client.sse import sse_client
            url = cfg.get("url")
            if not url:
                raise ValueError(f"server {name!r} sse without url")
            headers = _resolve_headers(cfg, name)
            log.info(f"[mcp:{name}] connecting sse: {url}")
            read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        else:
            raise ValueError(f"unknown transport {transport!r}")

        tools_resp = await session.list_tools()
        for tool in tools_resp.tools:
            prefixed = f"{name}__{tool.name}"
            tool_index[prefixed] = (session, tool, tool.name)
            # Cortex COMPLETE accepts OpenAI-format tool definitions (same as openai_agent).
            cortex_tools.append({
                "type": "function",
                "function": {
                    "name": prefixed,
                    "description": tool.description or "",
                    "parameters": tool.inputSchema or {"type": "object", "properties": {}},
                },
            })
        servers_used.append(name)
        log.info(f"[mcp:{name}] discovered {len(tools_resp.tools)} tools")
    return tool_index, cortex_tools, servers_used


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    account_url_env_var: str,
    auth_token_env_var: str,
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    request_timeout_seconds: int,
    mcp_servers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    import json
    import os
    from contextlib import AsyncExitStack

    try:
        import requests
    except ImportError:
        raise ImportError("pip install 'requests>=2.28'")

    account_url = os.environ.get(account_url_env_var, "").rstrip("/")
    if not account_url:
        raise RuntimeError(f"{account_url_env_var!r} not set.")
    token = os.environ.get(auth_token_env_var)
    if not token:
        raise RuntimeError(f"{auth_token_env_var!r} not set.")

    endpoint = f"{account_url}/api/v2/cortex/inference:complete"
    http_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # Cortex requires this token type header for PAT auth in some accounts.
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    }

    async with AsyncExitStack() as stack:
        tool_index, cortex_tools, servers_used = await _connect_mcp(stack, log, mcp_servers)

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        last_text = ""

        for i in range(max_iterations):
            iterations = i + 1
            payload: Dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if cortex_tools:
                payload["tools"] = cortex_tools

            resp = requests.post(
                endpoint,
                json=payload,
                headers=http_headers,
                timeout=request_timeout_seconds,
            )
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Cortex inference failed: HTTP {resp.status_code} {resp.text[:500]!r}"
                )
            body = resp.json()
            choice = body.get("choices", [{}])[0] or {}
            msg = choice.get("message") or {}
            tool_calls = msg.get("tool_calls") or []

            # Mirror the assistant turn into the running message list.
            assistant_msg: Dict[str, Any] = {"role": "assistant"}
            if msg.get("content"):
                assistant_msg["content"] = msg["content"]
                last_text = msg["content"]
            if tool_calls:
                assistant_msg["tool_calls"] = tool_calls
            messages.append(assistant_msg)

            if not tool_calls:
                stopped_reason = "final_answer"
                break

            for tc in tool_calls:
                fn = (tc.get("function") or {})
                fn_name = fn.get("name", "")
                raw_args = fn.get("arguments", "{}")
                try:
                    fn_args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
                except json.JSONDecodeError:
                    fn_args = {}

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered"
                    log.warning(err)
                    messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": f"Error: {err}"})
                    tool_call_details.append({"iteration": iterations, "tool": fn_name, "args": fn_args, "error": err})
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

                messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": tool_output})
                tool_call_details.append({
                    "iteration": iterations,
                    "tool": fn_name,
                    "args": fn_args,
                    "result_preview": tool_output[:500],
                    "is_error": is_error,
                })

        return {
            "final_answer": last_text,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": messages,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
        }
