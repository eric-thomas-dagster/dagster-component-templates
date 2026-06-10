"""AuditLogsToElasticSecurityComponent.

Ship audit-log DataFrame to Elastic Security via the bulk indexing API.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToElasticSecurityComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Elastic Security via the bulk indexing API."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    es_url: str = Field(description="Elasticsearch URL (e.g. 'https://es.acme.com:9200')")
    api_key_env: Optional[str] = Field(default=None, description="Env var with API key (preferred)")
    username_env: Optional[str] = Field(default=None, description="Basic-auth username env var")
    password_env: Optional[str] = Field(default=None, description="Basic-auth password env var")
    index: str = Field(default="logs-audit-default", description="Target index or data stream")
    verify_certs: bool = Field(default=True)

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
            description=self.description or "Ship audit-log DataFrame to Elastic Security via the bulk indexing API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['elastic', 'elasticsearch', 'siem']),
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
            from elasticsearch import Elasticsearch, helpers
            kw = {"verify_certs": _self.verify_certs}
            if _self.api_key_env:
                kw["api_key"] = os.environ[_self.api_key_env]
            elif _self.username_env and _self.password_env:
                kw["basic_auth"] = (os.environ[_self.username_env], os.environ[_self.password_env])
            es = Elasticsearch(_self.es_url, **kw)
            actions = ({"_index": _self.index, "_source": json.loads(row.to_json())} for _, row in df.iterrows())
            success, errors = helpers.bulk(es, actions, raise_on_error=False)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(int(success)),
                "errors": dg.MetadataValue.int(len(errors) if isinstance(errors, list) else 0),
                "index": dg.MetadataValue.text(_self.index),
            })

        return dg.Definitions(assets=[_asset])
