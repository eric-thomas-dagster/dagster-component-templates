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
from typing import Dict, List, Optional

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
        description="PromQL query, e.g. 'rate(http_requests_total[5m])' or 'up{job=\"node\"}'"
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
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
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
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

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

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
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
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"PromQL query against {cfg.server_url}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def prometheus_source(context: AssetExecutionContext) -> MaterializeResult:
            import requests

            headers = {}
            if cfg.bearer_token_env_var:
                token = os.environ[cfg.bearer_token_env_var]
                headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            auth = None
            if cfg.basic_auth_user_env_var and cfg.basic_auth_pass_env_var:
                auth = (
                    os.environ[cfg.basic_auth_user_env_var],
                    os.environ[cfg.basic_auth_pass_env_var],
                )

            base = cfg.server_url.rstrip("/")
            if cfg.range_query:
                end = datetime.utcnow()
                start = end - timedelta(seconds=cfg.range_seconds)
                params = {
                    "query": cfg.query,
                    "start": start.timestamp(),
                    "end": end.timestamp(),
                    "step": cfg.step_seconds,
                }
                resp = requests.get(f"{base}/api/v1/query_range", params=params, headers=headers, auth=auth, timeout=60)
            else:
                resp = requests.get(f"{base}/api/v1/query", params={"query": cfg.query}, headers=headers, auth=auth, timeout=60)
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
                    "query": MetadataValue.text(cfg.query),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[prometheus_source])
