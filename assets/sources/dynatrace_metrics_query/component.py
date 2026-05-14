"""Dynatrace Metrics → DataFrame.

Run a metrics query via Dynatrace Metrics API v2 and materialize the
result as a DataFrame asset. Each metric series → rows of (timestamp,
value, dimensions...).
"""

import os
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


class DynatraceMetricsQueryComponent(Component, Model, Resolvable):
    """Query Dynatrace metrics → DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    environment_url: str = Field(description="Dynatrace environment URL")
    api_token_env_var: str = Field(description="Env var holding the API token (metrics.read scope)")

    metric_selector: str = Field(
        description=(
            "Dynatrace metric selector. Example: "
            "'builtin:host.cpu.usage:splitBy(dt.entity.host):avg' or "
            "'builtin:tech.generic.cpu.usage:filter(eq(dt.entity.host,HOST-XXXX))'"
        )
    )
    resolution: str = Field(
        default="1m",
        description="Resolution: '1m', '5m', '1h', etc. Coarser resolutions cover longer time-frames.",
    )
    timeframe: str = Field(
        default="now-1h",
        description="Time range: relative (now-Nh / now-Nd) or absolute ISO 8601 (start_iso/end_iso)",
    )
    entity_selector: Optional[str] = Field(
        default=None,
        description="Optional entity scope filter, e.g. 'type(HOST),tag(\"env:prod\")'",
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
        kinds = self.kinds or ["dynatrace", "metrics", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Dynatrace metric: {cfg.metric_selector[:80]}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def dynatrace_query(context: AssetExecutionContext) -> MaterializeResult:
            import requests
            token = os.environ.get(cfg.api_token_env_var)
            if not token:
                raise RuntimeError(f"Missing {cfg.api_token_env_var}")
            url = f"{cfg.environment_url.rstrip('/')}/api/v2/metrics/query"

            params = {
                "metricSelector": cfg.metric_selector,
                "resolution": cfg.resolution,
                "from": cfg.timeframe,
            }
            if cfg.entity_selector:
                params["entitySelector"] = cfg.entity_selector

            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Api-Token {token}"},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            rows = []
            for result in data.get("result", []):
                metric_id = result.get("metricId", cfg.metric_selector)
                for series in result.get("data", []):
                    dims = series.get("dimensionMap", {}) or {}
                    timestamps = series.get("timestamps", [])
                    values = series.get("values", [])
                    for ts, v in zip(timestamps, values):
                        rows.append({
                            **dims,
                            "metric_id": metric_id,
                            "timestamp": pd.to_datetime(int(ts), unit="ms"),
                            "value": v,
                        })
            df = pd.DataFrame(rows)
            context.log.info(f"Dynatrace returned {len(df)} samples")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "metric_selector": MetadataValue.text(cfg.metric_selector),
                    "timeframe": MetadataValue.text(cfg.timeframe),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[dynatrace_query])
