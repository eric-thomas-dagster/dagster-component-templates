"""GcpAuditLogIngestionComponent.

Pull GCP audit logs via the Cloud Logging API. Returns a DataFrame.

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class GcpAuditLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull GCP audit logs via the Cloud Logging API. Returns a DataFrame."""

    asset_name: str = Field(description="Dagster asset name")

    project_id: str = Field(description="GCP project ID")
    log_filter: str = Field(default='logName:"cloudaudit.googleapis.com"', description="Cloud Logging advanced filter")
    lookback_hours: int = Field(default=24, description="How far back to fetch logs")
    page_size: int = Field(default=1000, description="Max entries per page")

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: str = Field(default="security_audit", description="Dagster asset group")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset deps")
    owners: Optional[list[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[dict] = Field(default=None, description="Catalog tags")
    kinds: Optional[list[str]] = Field(default=None, description="Asset kinds")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    include_preview_metadata: bool = Field(default=True, description="Emit preview metadata")
    preview_rows: int = Field(default=20, description="Preview row count")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )
        freshness = None
        if self.freshness_max_lag_minutes:
            freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Pull GCP audit logs via the Cloud Logging API. Returns a DataFrame.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['gcp', 'audit', 'logging']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            from google.cloud import logging as gcp_logging
            import datetime as dt
            client = gcp_logging.Client(project=_self.project_id)
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(hours=_self.lookback_hours)
            time_filter = f'timestamp >= "{start.isoformat()}Z" AND timestamp <= "{end.isoformat()}Z"'
            full_filter = f"({_self.log_filter}) AND ({time_filter})"
            entries = list(client.list_entries(filter_=full_filter, page_size=_self.page_size))
            rows = []
            for e in entries:
                rows.append({
                    "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                    "log_name": e.log_name,
                    "severity": e.severity,
                    "resource": str(e.resource),
                    "payload": json.dumps(e.payload) if e.payload else None,
                })
            df = pd.DataFrame(rows)
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(sample.to_markdown(index=False))
                except Exception as exc:
                    context.log.warning(f"preview emission failed: {exc}")
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
