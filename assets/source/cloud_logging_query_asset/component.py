"""CloudLoggingQueryAssetComponent — pull log entries via Cloud Logging filter language.

Returns a pandas DataFrame, one row per log entry, columns include timestamp,
severity, resource_type, log_name, text_payload / json_payload, labels. Useful
for downstream alerting, error-rate analytics, audit-event ingestion, or as a
trigger asset for sensors that watch for specific log patterns.

Filter syntax follows the Cloud Logging query language:
  https://cloud.google.com/logging/docs/view/logging-query-language

Example filters:
  severity>=ERROR
  resource.type="cloud_run_revision" AND severity=ERROR
  protoPayload.methodName="storage.objects.delete"
  logName="projects/<proj>/logs/cloudaudit.googleapis.com%2Factivity"
"""

import datetime as dt
import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class CloudLoggingQueryAssetComponent(Component, Model, Resolvable):
    """Run a Cloud Logging filter and return matching entries as a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="GCP project to query. Default: project from credentials.")

    filter_: str = Field(
        alias="filter",
        description=(
            "Cloud Logging filter expression. e.g. 'severity>=ERROR' or "
            "'resource.type=\"cloud_run_revision\" AND severity=ERROR'."
        ),
    )

    lookback_minutes: Optional[int] = Field(
        default=None,
        description=(
            "If set, automatically anchor the filter to entries from the last N "
            "minutes (appends `timestamp>=\"<ISO>\"`). Use this OR put your own "
            "timestamp clause in `filter` — not both."
        ),
    )

    order_by: Literal["timestamp asc", "timestamp desc"] = Field(
        default="timestamp desc",
        description="Cloud Logging order_by clause.",
    )

    max_entries: int = Field(default=1000, description="Cap on total entries returned (uses page iteration).")

    resource_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of resource scopes (e.g. ['projects/foo', 'folders/123']). "
            "Default: ['projects/<project_id>']."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

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

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        filter_text = self.filter_
        lookback_minutes = self.lookback_minutes
        order_by = self.order_by
        max_entries = self.max_entries
        resource_names = self.resource_names or [f"projects/{project_id}"]

        @asset(
            name=asset_name,
            description=self.description or f"Cloud Logging query in {project_id}.",
            group_name=self.group_name,
            kinds={"google", "cloud-logging"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import logging as gcl
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-logging google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcl.Client(project=project_id, credentials=sa_creds)

            final_filter = filter_text
            if lookback_minutes is not None:
                since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=lookback_minutes)
                ts_clause = f'timestamp>="{since.strftime("%Y-%m-%dT%H:%M:%SZ")}"'
                final_filter = f"({filter_text}) AND {ts_clause}" if filter_text.strip() else ts_clause

            context.log.info(f"Cloud Logging filter: {final_filter}")
            context.log.info(f"Scopes: {resource_names}")

            rows: List[Dict[str, Any]] = []
            entries_iter = client.list_entries(
                resource_names=resource_names,
                filter_=final_filter,
                order_by=order_by,
                page_size=min(1000, max_entries),
            )
            for entry in entries_iter:
                if len(rows) >= max_entries:
                    break
                payload = entry.payload
                text_payload = payload if isinstance(payload, str) else None
                json_payload = payload if isinstance(payload, dict) else None
                rows.append({
                    "timestamp":     entry.timestamp,
                    "severity":      entry.severity,
                    "log_name":      entry.log_name,
                    "resource_type": entry.resource.type if entry.resource else None,
                    "resource_labels": dict(entry.resource.labels) if entry.resource and entry.resource.labels else None,
                    "text_payload":  text_payload,
                    "json_payload":  json_payload,
                    "labels":        dict(entry.labels) if entry.labels else None,
                    "trace":         entry.trace,
                    "insert_id":     entry.insert_id,
                })

            df = pd.DataFrame(rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no entries)"
            sev_counts = df["severity"].value_counts().to_dict() if not df.empty and "severity" in df.columns else {}
            return Output(
                value=df,
                metadata={
                    "filter":      MetadataValue.text(final_filter),
                    "scopes":      MetadataValue.json(resource_names),
                    "entry_count": MetadataValue.int(len(df)),
                    "by_severity": MetadataValue.json({str(k): int(v) for k, v in sev_counts.items()}),
                    "preview":     MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
