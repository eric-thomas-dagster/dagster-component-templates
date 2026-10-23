"""MCP Tool Call Component.

Deterministic single-shot call to one MCP tool — no LLM in the loop.

Use this when you know exactly which tool to call and what args to pass.
The component connects to one MCP server (stdio / http / sse), calls the
named tool with the supplied args, parses the response, and materializes
the result as an asset.

Args templating: `{partition_key}`, `{partition_keys.<dim>}`, and `{run_id}`
are substituted into `tool_args` at materialization time, so partitioned
schedules + multi-dim partitions both work cleanly. Substitution is done
on string leaves only — numeric / bool values pass through unchanged.

Output: parsed tool response (dict / list / str depending on `parse_as`),
with materialization metadata for tool name, args, result size, and a
markdown preview.
"""
from typing import Any, Dict, List, Literal, Optional
import dagster as dg
from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
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
    name: str = Field(description="Short label for logs (also used as the asset's MCP server tag).")
    type: str = Field(default="stdio", description="Transport: 'stdio' | 'http' | 'sse'.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL of the MCP endpoint.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: map of header_name → env_var_name (value read from env)."
    )


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Same shape as the other components — strict combinations only."""
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
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
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
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
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
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



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


class MCPToolCallComponent(Component, Model, Resolvable):
    """Deterministic single-shot call to one MCP tool — no LLM.

    Example:
        ```yaml
        type: dagster_component_templates.MCPToolCallComponent
        attributes:
          asset_name: dagster_plus_recent_failures
          server:
            name: dgp
            type: http
            url: https://mcp.agent.dagster.cloud/mcp/
            headers:
              Dagster-Cloud-Organization: my-org
            headers_env:
              Authorization: DAGSTER_PLUS_BEARER
          tool_name: list_runs
          tool_args:
            limit: 10
            status: FAILURE
            deployment_name: prod
          parse_as: json
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    server: MCPServerSpec = Field(description="MCP server to connect to.")
    tool_name: str = Field(description="Name of the MCP tool to call.")
    tool_args: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Args passed to the tool. String leaves get template substitution: `{partition_key}`, `{partition_keys.<dim>}`, `{run_id}`.",
    )
    parse_as: str = Field(
        default="auto",
        description="How to parse the tool's text response: 'json' (raise if not JSON), 'text' (raw string), 'auto' (try JSON, fall back to text).",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['mcp', 'python']. Auto-includes 'mcp' if not set.",
    )

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'multi' | 'dynamic'")

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
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values} dicts.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (one tool call "
            "per partition in the range, each with its own {partition_key} "
            "substitution; the asset's value is a dict {partition_key: result}). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when no "
            "partitioning is configured. 'single_run' requires a "
            "partition-range-aware IO manager -- a generic filesystem/default "
            "IO manager cannot persist a single_run output spanning multiple "
            "partitions and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        server = self.server
        tool_name = self.tool_name
        tool_args = self.tool_args or {}
        parse_as = self.parse_as
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        # Infer kinds — always include 'mcp'.
        _inferred_kinds = list(self.kinds or [])
        if "mcp" not in _inferred_kinds:
            _inferred_kinds.append("mcp")
        if not _inferred_kinds:
            _inferred_kinds = ["python"]

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            # Modern Dagster: FreshnessPolicy is constructed via class methods.
            # - cron schedule + max-lag-after-deadline → FreshnessPolicy.cron(...)
            # - just a max-lag window → FreshnessPolicy.time_window(...)
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            if self.freshness_cron:
                _freshness_policy = FreshnessPolicy.cron(
                    deadline_cron=self.freshness_cron,
                    lower_bound_delta=_lag,
                )
            else:
                _freshness_policy = FreshnessPolicy.time_window(fail_window=_lag)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _tool_call_asset(context: AssetExecutionContext) -> Any:
            import asyncio

            # Partition key(s) this call must process: a normal tick gives
            # exactly one; a single_run backfill gives the whole range, and
            # each key gets its own tool call with its own substitutions.
            is_range = (not context.has_partition_key) and context.has_partition_key_range
            keys: List[Any] = [None]
            if context.has_partition_key or is_range:
                keys = list(context.partition_keys)

            def _call_for(pk):
                substitutions = {"run_id": context.run_id}
                if pk is not None:
                    if hasattr(pk, "keys_by_dimension"):
                        substitutions["partition_key"] = str(pk)
                        substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                    else:
                        substitutions["partition_key"] = str(pk)
                        substitutions["partition_keys"] = {}
                resolved = _substitute_args(tool_args, substitutions)
                res = asyncio.run(
                    _call_tool(
                        log=context.log,
                        server_cfg=server.model_dump(),
                        tool_name=tool_name,
                        tool_args=resolved,
                        parse_as=parse_as,
                    )
                )
                if res["is_error"]:
                    raise RuntimeError(
                        f"MCP tool {tool_name!r} returned isError=true"
                        + (f" for partition {pk!r}" if pk is not None else "")
                        + f": {res['raw'][:500]}"
                    )
                return resolved, res

            calls = [(pk,) + _call_for(pk) for pk in keys]
            resolved_args = calls[0][1] if not is_range else {str(pk): r for pk, r, _ in calls}
            result = calls[0][2]

            md = {
                "mcp_server": MetadataValue.text(server.name),
                "tool_name": MetadataValue.text(tool_name),
                "tool_args": MetadataValue.json(resolved_args),
                "is_error": MetadataValue.bool(False),
                "result_kind": MetadataValue.text(result["kind"]),
                "result_size_bytes": MetadataValue.int(sum(c[2]["raw_size"] for c in calls)),
            }
            if is_range:
                md["partition_keys"] = MetadataValue.json([str(pk) for pk in keys])
            if not is_range and result["kind"] == "json":
                if isinstance(result["value"], list):
                    md["result_item_count"] = MetadataValue.int(len(result["value"]))
                elif isinstance(result["value"], dict):
                    md["result_key_count"] = MetadataValue.int(len(result["value"]))
            # Render a markdown preview of the response so the Dagster UI is useful.
            _raw = result["raw"] if not is_range else "\n".join(f"[{pk}] {c[2]['raw'][:500]}" for pk, c in zip(keys, calls))
            md["preview"] = MetadataValue.md(
                f"```\n{_raw[:2000]}\n```"
                + ("" if len(_raw) <= 2000 else f"\n\n_(truncated — full response is {len(_raw)} bytes)_")
            )
            context.add_output_metadata(md)

            if is_range:
                # Multi-partition output: {partition_key: tool result}
                # (Dagster's own UPathIOManager convention for ranges).
                return {str(pk): c[2]["value"] for pk, c in zip(keys, calls)}
            return result["value"]

        return Definitions(assets=[_tool_call_asset])


def _substitute_args(args: Any, substitutions: Dict[str, Any]) -> Any:
    """Recursively substitute `{partition_key}` / `{partition_keys.X}` / `{run_id}` in string leaves."""
    if isinstance(args, dict):
        return {k: _substitute_args(v, substitutions) for k, v in args.items()}
    if isinstance(args, list):
        return [_substitute_args(v, substitutions) for v in args]
    if isinstance(args, str):
        # Quick-exit if no template markers.
        if "{" not in args:
            return args
        out = args
        out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
        out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
        for dim, val in (substitutions.get("partition_keys") or {}).items():
            out = out.replace("{partition_keys." + dim + "}", str(val))
        return out
    return args


def _resolve_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    import os
    headers: Dict[str, str] = {}
    for k, v in (cfg.get("headers") or {}).items():
        headers[k] = str(v)
    for header_name, env_var in (cfg.get("headers_env") or {}).items():
        val = os.environ.get(env_var)
        if val is None:
            raise ValueError(
                f"MCP server {server_name!r} references env var {env_var!r} for header "
                f"{header_name!r}, but it isn't set."
            )
        headers[header_name] = val
    return headers


async def _call_tool(
    log,
    server_cfg: Dict[str, Any],
    tool_name: str,
    tool_args: Dict[str, Any],
    parse_as: str,
) -> Dict[str, Any]:
    """Connect, call, parse, return {value, raw, raw_size, is_error, kind}."""
    import json
    from contextlib import AsyncExitStack

    name = server_cfg["name"]
    transport = server_cfg.get("type", "stdio")

    async with AsyncExitStack() as stack:
        if transport == "stdio":
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client

            cmd = server_cfg.get("command") or []
            if not cmd:
                raise ValueError(f"MCP server {name!r} is stdio but command is empty.")
            params = StdioServerParameters(command=cmd[0], args=list(cmd[1:]), env=server_cfg.get("env"))
            log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport in ("http", "streamable_http", "streamable-http"):
            from mcp import ClientSession
            # mcp 1.x named this streamablehttp_client; mcp 2.x renamed to
            # streamable_http_client. Try new name first, fall back to old.
            try:
                from mcp.client.streamable_http import streamable_http_client as _http_client
            except ImportError:
                from mcp.client.streamable_http import streamablehttp_client as _http_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is http but url is empty.")
            headers = _resolve_headers(server_cfg, name)
            log.info(
                f"[mcp:{name}] connecting via streamable-http: {url}"
                + (f" (headers: {sorted(headers.keys())})" if headers else "")
            )
            read, write, _sid = await stack.enter_async_context(
                _http_client(url, headers=headers or None)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "sse":
            from mcp import ClientSession
            from mcp.client.sse import sse_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is sse but url is empty.")
            headers = _resolve_headers(server_cfg, name)
            log.info(
                f"[mcp:{name}] connecting to sse: {url}"
                + (f" (headers: {sorted(headers.keys())})" if headers else "")
            )
            read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        else:
            raise ValueError(f"MCP server {name!r} has unknown transport: {transport!r}")

        log.info(f"[mcp:{name}] calling {tool_name}({tool_args!r})")
        call_result = await session.call_tool(tool_name, tool_args)
        parts = []
        for c in call_result.content:
            text = getattr(c, "text", None)
            parts.append(text if text is not None else str(c))
        raw = "\n".join(parts) if parts else ""
        is_error = bool(getattr(call_result, "isError", False))

    # Parse the response.
    if parse_as == "text":
        return {"value": raw, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "text"}
    if parse_as in ("json", "auto"):
        try:
            value = json.loads(raw)
            return {"value": value, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "json"}
        except (json.JSONDecodeError, ValueError):
            if parse_as == "json":
                raise RuntimeError(f"MCP tool {tool_name!r} returned non-JSON output; raw[:500]={raw[:500]!r}")
            # auto → fall through to text
            return {"value": raw, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "text"}
    raise ValueError(f"unknown parse_as: {parse_as!r} (expected 'json' | 'text' | 'auto')")
