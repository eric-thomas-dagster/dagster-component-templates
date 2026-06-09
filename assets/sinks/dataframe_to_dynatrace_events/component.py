"""DataFrame → Dynatrace Events.

Push DataFrame rows as generic Dynatrace events via the v2 Events API.
Use this for batch-job markers (deployments, ETL completions, anomalies)
that should annotate Dynatrace's timeline alongside operational metrics.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
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


class DataframeToDynatraceEventsComponent(Component, Model, Resolvable):
    """Ship DataFrame rows as Dynatrace v2 events."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    environment_url: str = Field(description="Dynatrace environment URL")
    api_token_env_var: str = Field(description="Env var holding the API token (events.ingest scope)")

    event_type: str = Field(
        default="CUSTOM_INFO",
        description="Dynatrace event type: 'CUSTOM_INFO' | 'CUSTOM_DEPLOYMENT' | 'CUSTOM_ANNOTATION' | 'AVAILABILITY_EVENT' | 'ERROR_EVENT' | 'PERFORMANCE_EVENT' | 'RESOURCE_CONTENTION_EVENT'",
    )
    title_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the event title. Default: synthesized from row.",
    )
    entity_selector: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dynatrace entity selector to attach events to. "
            "Example: 'type(HOST),tag(\"env:prod\")'. Required if you want events on a specific entity."
        ),
    )
    properties_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns whose values become custom event properties. Default: all columns.",
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
        kinds = self.kinds or ["dynatrace", "events", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Push DataFrame rows as Dynatrace events",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def dynatrace_events(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            import requests
            token = os.environ.get(cfg.api_token_env_var)
            if not token:
                raise RuntimeError(f"Missing {cfg.api_token_env_var}")
            url = f"{cfg.environment_url.rstrip('/')}/api/v2/events/ingest"

            sent, failed = 0, 0
            records = upstream.to_dict(orient="records")
            prop_cols = cfg.properties_columns or list(upstream.columns)

            for r in records:
                title = str(r.get(cfg.title_column)) if cfg.title_column else f"Dagster event: {r.get(prop_cols[0]) if prop_cols else 'row'}"
                payload = {
                    "eventType": cfg.event_type,
                    "title": title,
                    "properties": {
                        c: str(r[c]) for c in prop_cols if c in r and r[c] is not None and not pd.isna(r[c])
                    },
                }
                if cfg.entity_selector:
                    payload["entitySelector"] = cfg.entity_selector
                resp = requests.post(
                    url,
                    json=payload,
                    headers={"Authorization": f"Api-Token {token}", "Content-Type": "application/json"},
                    timeout=30,
                )
                if 200 <= resp.status_code < 300:
                    sent += 1
                else:
                    failed += 1
                    context.log.warning(f"Dynatrace event failed: {resp.status_code} {resp.text[:200]}")

            context.log.info(f"Pushed {sent}/{len(records)} events to Dynatrace")
            return MaterializeResult(
                metadata={
                    "events_sent": MetadataValue.int(sent),
                    "events_failed": MetadataValue.int(failed),
                    "event_type": MetadataValue.text(cfg.event_type),
                    "environment_url": MetadataValue.text(cfg.environment_url),
                }
            )

        return Definitions(assets=[dynatrace_events])
