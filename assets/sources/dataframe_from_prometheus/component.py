"""Prometheus → DataFrame (PromQL query source).

Run a PromQL query against any Prometheus-compatible server's HTTP API
(/api/v1/query for instant, /api/v1/query_range for range) and
materialize the result as a DataFrame asset.

Works against:
- Open-source Prometheus
- Cortex / Thanos / Mimir
- VictoriaMetrics
- **Azure Managed Prometheus** (set bearer_token_env_var to a token
  from `az account get-access-token --resource <prometheus-endpoint>`)
- Grafana Cloud
- AWS Managed Prometheus (with sigv4 sidecar)

For PUSHING metrics, see the companion `dataframe_to_prometheus` sink
or the existing `prometheus_resource` (pushgateway).
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field



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


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


class DataframeFromPrometheusComponent(Component, Model, Resolvable):
    """Run a PromQL query and materialize the result as a DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")
    server_url: str = Field(
        description=(
            "Prometheus base URL (no trailing slash), e.g. "
            "'https://prometheus.demo.do.prometheus.io', "
            "'https://my-managed-prometheus.eastus.prometheus.monitor.azure.com', "
            "'http://localhost:9090'"
        )
    )
    query: str = Field(
        description="PromQL query, e.g. 'rate(http_requests_total[5m])' or 'up{job=\"node\"}' On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
    )

    range_query: bool = Field(
        default=False,
        description=(
            "False (default): instant query at time 'now' (single value per series). "
            "True: range query (returns a time series per matching series)."
        ),
    )
    range_seconds: int = Field(
        default=3600,
        description="Range duration in seconds (only used when range_query=true). Default: last hour.",
    )
    step_seconds: int = Field(
        default=60,
        description="Step (sample interval) in seconds for range queries. Default: 60s.",
    )

    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a Bearer token for Authorization header. Required for "
            "Azure Managed Prometheus, Grafana Cloud, and any auth-protected server. "
            "For Azure Managed Prometheus: az account get-access-token "
            "--resource https://prometheus.monitor.azure.com"
        ),
    )
    basic_auth_user_env_var: Optional[str] = Field(default=None)
    basic_auth_pass_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional headers (e.g. X-Scope-OrgID for Cortex/Mimir tenants)",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires one query covering the whole backfill range (the "
            "partition_key/partition_date/partition_start/partition_end tokens "
            "reflect the full range, comma-joined for partition_key) rather than "
            "one query per partition. 'multi_run' launches one run per partition "
            "(optionally capped via backfill_policy_max_partitions_per_run). Leave "
            "unset for Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem/default IO manager cannot persist a "
            "single_run output spanning multiple partitions and will raise a clear "
            "error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        cfg = self
        kinds = self.kinds or ["prometheus", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or f"PromQL query against {cfg.server_url}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            backfill_policy=backfill_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def prometheus_source(context: AssetExecutionContext) -> MaterializeResult:
            import requests

            headers = {}
            if cfg.bearer_token_env_var:
                token = _require_env(cfg.bearer_token_env_var)
                headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            auth = None
            if cfg.basic_auth_user_env_var and cfg.basic_auth_pass_env_var:
                auth = (
                    _require_env(cfg.basic_auth_user_env_var),
                    _require_env(cfg.basic_auth_pass_env_var),
                )

            base = cfg.server_url.rstrip("/")
            if cfg.range_query:
                end = datetime.utcnow()
                start = end - timedelta(seconds=cfg.range_seconds)
                params = {
                    "query": _substitute_partition(cfg.query, _partition_vars(context)),
                    "start": start.timestamp(),
                    "end": end.timestamp(),
                    "step": cfg.step_seconds,
                }
                resp = requests.get(f"{base}/api/v1/query_range", params=params, headers=headers, auth=auth, timeout=60)
            else:
                resp = requests.get(f"{base}/api/v1/query", params={"query": _substitute_partition(cfg.query, _partition_vars(context))}, headers=headers, auth=auth, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                raise Exception(f"PromQL error: {data.get('error', 'unknown')}")

            result_type = data["data"]["resultType"]
            results = data["data"]["result"]

            rows = []
            for series in results:
                metric = series.get("metric", {})
                if result_type == "matrix":
                    for ts, val in series.get("values", []):
                        rows.append({**metric, "timestamp": pd.to_datetime(float(ts), unit="s"), "value": float(val)})
                elif result_type == "vector":
                    ts, val = series.get("value", [None, None])
                    if ts is not None:
                        rows.append({**metric, "timestamp": pd.to_datetime(float(ts), unit="s"), "value": float(val)})
                elif result_type in ("scalar", "string"):
                    ts, val = series if isinstance(series, list) else (None, None)
                    if ts is not None:
                        rows.append({"timestamp": pd.to_datetime(float(ts), unit="s"), "value": val})

            df = pd.DataFrame(rows)
            context.log.info(f"PromQL returned {len(df)} samples ({result_type})")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "result_type": MetadataValue.text(result_type),
                    "server": MetadataValue.text(cfg.server_url),
                    "query": MetadataValue.text(_substitute_partition(cfg.query, _partition_vars(context))),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[prometheus_source])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
