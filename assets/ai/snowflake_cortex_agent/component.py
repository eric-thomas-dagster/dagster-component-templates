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
import re
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
from pydantic import ConfigDict, Field, model_validator


class MCPServerSpec(dg.Model, dg.Resolvable):
    """Same MCPServerSpec shape as the other *_agent components.

    Declared inline (set `name` + the transport-specific fields), or shared via
    `server_resource_key` (an MCPServerResource component's resource_key) instead --
    exactly one of the two is required per entry.
    """
    name: Optional[str] = Field(
        default=None,
        description="Tool-name prefix. Required when declaring the server inline; leave unset when using `server_resource_key`. Exactly one of `name` (inline spec) or `server_resource_key` is required.",
    )
    type: str = Field(default="stdio", description="'stdio' | 'http' | 'sse'.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: header_name → env_var_name."
    )
    server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key of an MCPServerResource component to use instead of declaring "
            "this server inline -- lets several agent components (and mcp_tool_call / "
            "mcp_tool_picker) share one registered server. Exactly one of `name` (inline "
            "spec) or `server_resource_key` is required."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_server_source(self):
        if bool(self.name) == bool(self.server_resource_key):
            raise ValueError(
                "MCPServerSpec requires exactly one of `name` (+ transport fields, declared "
                "inline) or `server_resource_key` (an MCPServerResource component's resource_key)."
            )
        return self



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


class SnowflakeCortexAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using Snowflake Cortex COMPLETE + MCP tools.

    Example:
        ```yaml
        type: dagster_community_components.SnowflakeCortexAgentComponent
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
    allowed_tools: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), "
            "'*' wildcards supported (fnmatch), applied across every mcp_servers entry. "
            "Default: all discovered tools. A server can expose many tools the agent "
            "shouldn't be able to call (e.g. a hosted Dagster+ MCP server's "
            "terminate_run/rerun_run/delete_alert_policy alongside read-only ones) -- this "
            "is the lever to restrict an agent to a safe subset, e.g. "
            "['dagster_plus__list_*', 'dagster_plus__get_*']."
        ),
    )
    skill_keys: Optional[List[str]] = Field(
        default=None,
        description="SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill.",
    )
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['llm', 'agent', 'snowflake', 'cortex'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only deps.")

    # Partitions (same shape as the other agents).
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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="CSV.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Dynamic partition name.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis spec.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Delay between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' | 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps', 'skill_keys'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Snowflake Cortex Agent', editable=True)

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
        allowed_tools = self.allowed_tools
        skill_keys = self.skill_keys
        request_timeout_seconds = self.request_timeout_seconds
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        _inferred_kinds = list(self.kinds or ["llm", "agent", "snowflake", "cortex"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""
        if self.skill_keys:
            # Dagster's strict tag-value validation forbids commas -- sanitize before
            # setdefault so 2+ skill_keys doesn't crash build_defs.
            _all_tags.setdefault("dagster-community/skill", _safe_tag_value(",".join(sorted(self.skill_keys))))

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

        # Union of every mcp_servers[].server_resource_key in use -- resolved once at
        # build_defs time (the YAML is static even though which path each entry takes
        # is only read at materialization time).
        _mcp_resource_keys = {s.server_resource_key for s in mcp_servers if s.server_resource_key}
        _skill_resource_keys = set(skill_keys or [])
        _required_resource_keys = _mcp_resource_keys | _skill_resource_keys

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
            required_resource_keys=_required_resource_keys or None,
        )
        def _agent_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            import asyncio

            # Resolved once per run: each entry's shared MCPServerResource config, or
            # its own inline spec -- either way the same dict shape _run_agent expects.
            resolved_mcp_servers = [
                getattr(context.resources, s.server_resource_key).model_dump()
                if s.server_resource_key
                else s.model_dump()
                for s in mcp_servers
            ]

            # Resolve each configured skill_keys entry to its registered SkillResource,
            # keyed by the skill's resolved NAME (what the model sees + what it passes
            # to load_skill/read_skill_file) -- not the Dagster resource_key. This is the
            # earliest point this component's own code can check that the key resolves to
            # a real skill resource: Dagster only binds cross-component resources at run
            # time, not at this component's own build_defs/parse time.
            skill_resources: Dict[str, Any] = {}
            for key in (skill_keys or []):
                try:
                    res = getattr(context.resources, key)
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"skill_keys references {key!r} but no resource is registered under that key: {e}"
                    ) from e
                if not (hasattr(res, "describe") and hasattr(res, "load")):
                    raise ValueError(
                        f"skill_keys references {key!r}, but the resource registered under "
                        f"that key is not a SkillResource (missing describe()/load())."
                    )
                info = res.describe()
                skill_resources[info["name"]] = res

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

            # Skills go into context up front as just name + description (cheap); the
            # full SKILL.md body only loads if the model actually calls load_skill.
            if skill_resources:
                _skill_lines = "\n".join(
                    f"- {name}: {res.describe()['description']}" for name, res in skill_resources.items()
                )
                _skill_listing = (
                    "\n\nAvailable skills (call load_skill with the skill's name to load "
                    f"its full instructions before using it):\n{_skill_lines}"
                )
                resolved_system = (resolved_system or "") + _skill_listing

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
                    mcp_servers=resolved_mcp_servers,
                    skill_resources=skill_resources,
                    allowed_tools=allowed_tools,
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
            if skill_keys:
                md["skills_available"] = MetadataValue.json(
                    [res.describe() for res in skill_resources.values()]
                )
                md["skills_loaded"] = MetadataValue.json(sorted(result.get("loaded_skill_names") or []))
                md["skill_load_count"] = MetadataValue.int(result.get("skill_load_count") or 0)
            context.add_output_metadata(md)
            return result

        return Definitions(assets=[_agent_asset])


def _safe_tag_value(value: str) -> str:
    """Dagster's strict tag-value validation only allows alpha-numeric/'_'/'-'/'.', <=63 chars
    -- a comma-joined skill_keys list (or any other free-text value) would otherwise crash at
    build_defs time with DagsterInvalidDefinitionError. Same sanitizer shape as
    openapi_tool_server's own _safe_tag_value."""
    v = re.sub(r"[^A-Za-z0-9_.\-]", "_", str(value))
    v = re.sub(r"_+", "_", v).strip("_")[:63]
    return v or "_"


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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start'")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _vals: raise ValueError("static requires values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name: raise ValueError("dynamic requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _vals or not partition_start: raise ValueError("multi requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
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


def _tool_name_allowed(name: str, allowed: Optional[List[str]]) -> bool:
    """`allowed` is a list of names/fnmatch patterns ('*' wildcards) matched against the
    namespaced tool name ('{server}__{tool}'). No allowlist = everything allowed."""
    if not allowed:
        return True
    import fnmatch
    return any(fnmatch.fnmatch(name, pattern) for pattern in allowed)


async def _connect_mcp(stack, log, mcp_servers, allowed_tools=None):
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
            url = cfg.get("url")
            if not url:
                raise ValueError(f"server {name!r} http without url")
            headers = _resolve_headers(cfg, name)
            log.info(f"[mcp:{name}] connecting http: {url}")
            read, write = await _open_mcp_streamable_http(stack, url, headers)
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
            if not _tool_name_allowed(prefixed, allowed_tools):
                continue
            tool_index[prefixed] = (session, tool, tool.name)
            # Cortex COMPLETE accepts OpenAI-format tool definitions (same as openai_agent).
            cortex_tools.append({
                "type": "function",
                "function": {
                    "name": prefixed,
                    "description": tool.description or "",
                    "parameters": _mcp_field(tool, "inputSchema", None) or {"type": "object", "properties": {}},
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
    skill_resources: Optional[Dict[str, Any]] = None,
    allowed_tools: Optional[List[str]] = None,
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
        tool_index, cortex_tools, servers_used = await _connect_mcp(stack, log, mcp_servers, allowed_tools)

        loaded_skill_names: set = set()
        skill_load_count = 0
        if skill_resources:
            cortex_tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": "load_skill",
                        "description": "Load the full SKILL.md instructions for a skill, by its name, before using it. Only call this once you've decided a skill is actually relevant.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "The skill's name, exactly as shown in the available-skills listing.",
                                }
                            },
                            "required": ["name"],
                        },
                    },
                }
            )
            cortex_tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": "read_skill_file",
                        "description": "Read one bundled file from a skill's folder, by path relative to that folder (see the `files` list returned by load_skill).",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "The skill's name, exactly as shown in the available-skills listing.",
                                },
                                "relative_path": {
                                    "type": "string",
                                    "description": "Path to the file, relative to the skill's folder.",
                                },
                            },
                            "required": ["name", "relative_path"],
                        },
                    },
                }
            )

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

                if fn_name == "load_skill":
                    skill_name = fn_args.get("name")
                    skill_res = (skill_resources or {}).get(skill_name)
                    if skill_res is None:
                        err = f"load_skill: no skill named {skill_name!r} is available"
                        log.warning(err)
                        tool_output, is_error = f"Error: {err}", True
                    else:
                        loaded = skill_res.load()
                        tool_output, is_error = loaded["content"], False
                        loaded_skill_names.add(skill_name)
                        skill_load_count += 1
                    messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": tool_output})
                    tool_call_details.append({
                        "iteration": iterations, "tool": fn_name, "args": fn_args,
                        "result_preview": tool_output[:500], "is_error": is_error,
                    })
                    continue

                if fn_name == "read_skill_file":
                    skill_name = fn_args.get("name")
                    relative_path = fn_args.get("relative_path")
                    skill_res = (skill_resources or {}).get(skill_name)
                    if skill_res is None:
                        err = f"read_skill_file: no skill named {skill_name!r} is available"
                        log.warning(err)
                        tool_output, is_error = f"Error: {err}", True
                    else:
                        try:
                            tool_output, is_error = skill_res.read_file(relative_path), False
                        except (ValueError, FileNotFoundError) as e:
                            tool_output, is_error = f"Error: {e}", True
                    messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": tool_output})
                    tool_call_details.append({
                        "iteration": iterations, "tool": fn_name, "args": fn_args,
                        "result_preview": tool_output[:500], "is_error": is_error,
                    })
                    continue

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
                    parts, is_error = _mcp_result_parts(call_result)
                    tool_output = "\n".join(parts) if parts else "(empty)"
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
            "loaded_skill_names": sorted(loaded_skill_names),
            "skill_load_count": skill_load_count,
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
