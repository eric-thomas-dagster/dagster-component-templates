"""AuditLogsToSumoLogicComponent.

Ship audit-log DataFrame to Sumo Logic via an HTTP Hosted Collector.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToSumoLogicComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Sumo Logic via an HTTP Hosted Collector."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    collector_url_env: str = Field(default="SUMO_HTTP_URL", description="Env var with the full HTTP Source URL")
    source_category: Optional[str] = Field(default=None, description="Override X-Sumo-Category")
    source_name: Optional[str] = Field(default=None, description="Override X-Sumo-Name")
    source_host: Optional[str] = Field(default=None, description="Override X-Sumo-Host")
    batch_size: int = Field(default=1000)

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_sink", description="Dagster asset group")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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

        _self = self
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Ship audit-log DataFrame to Sumo Logic via an HTTP Hosted Collector.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['sumo-logic', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            import requests
            url = os.environ[_self.collector_url_env]
            headers = {"Content-Type": "application/json"}
            if _self.source_category: headers["X-Sumo-Category"] = _self.source_category
            if _self.source_name: headers["X-Sumo-Name"] = _self.source_name
            if _self.source_host: headers["X-Sumo-Host"] = _self.source_host
            sent = 0
            for i in range(0, len(df), _self.batch_size):
                chunk = df.iloc[i : i + _self.batch_size]
                body = "\n".join(row.to_json() for _, row in chunk.iterrows())
                r = requests.post(url, data=body, headers=headers, timeout=60)
                if r.status_code >= 300:
                    raise Exception(f"Sumo Logic error: {r.status_code} {r.text[:200]}")
                sent += len(chunk)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "source_category": dg.MetadataValue.text(_self.source_category or "(default)"),
            })

        return dg.Definitions(assets=[_asset])
