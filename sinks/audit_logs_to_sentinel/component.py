"""AuditLogsToSentinelComponent.

Ship audit-log DataFrame to Microsoft Sentinel via the Log Analytics Data Collector API.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToSentinelComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Microsoft Sentinel via the Log Analytics Data Collector API."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    workspace_id: str = Field(description="Log Analytics workspace ID (GUID)")
    workspace_key_env: str = Field(default="SENTINEL_WORKSPACE_KEY", description="Env var with primary/secondary key")
    log_type: str = Field(default="DagsterAudit", description="Custom Logs table name (Sentinel will append _CL)")
    batch_size: int = Field(default=500)

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
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Ship audit-log DataFrame to Microsoft Sentinel via the Log Analytics Data Collector API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['sentinel', 'azure', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, df: Any) -> dg.MaterializeResult:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(df, dict):
                _frames = [v for v in df.values() if isinstance(v, pd.DataFrame)]
                df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            import requests, hashlib, hmac, base64, datetime as dt
            key = os.environ[_self.workspace_key_env]
            sent = 0
            for i in range(0, len(df), _self.batch_size):
                chunk = df.iloc[i : i + _self.batch_size]
                body = chunk.to_json(orient="records")
                now = dt.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
                content_length = len(body)
                string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{now}\n/api/logs"
                decoded = base64.b64decode(key)
                sig = base64.b64encode(hmac.new(decoded, string_to_hash.encode(), hashlib.sha256).digest()).decode()
                auth = f"SharedKey {_self.workspace_id}:{sig}"
                url = f"https://{_self.workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
                r = requests.post(url, data=body, headers={
                    "content-type": "application/json",
                    "Authorization": auth,
                    "Log-Type": _self.log_type,
                    "x-ms-date": now,
                }, timeout=60)
                if r.status_code >= 300:
                    raise Exception(f"Sentinel ingestion error: {r.status_code} {r.text[:200]}")
                sent += len(chunk)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "log_type": dg.MetadataValue.text(_self.log_type),
            })

        return dg.Definitions(assets=[_asset])
