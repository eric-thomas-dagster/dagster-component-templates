"""AzureActivityLogIngestionComponent.

Pull Azure Activity Log entries (control-plane audit) via the Monitor REST API.

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AzureActivityLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Azure Activity Log entries (control-plane audit) via the Monitor REST API."""

    asset_name: str = Field(description="Dagster asset name")

    subscription_id: Optional[str] = Field(default=None, description="Azure subscription ID (env: AZURE_SUBSCRIPTION_ID)")
    lookback_hours: int = Field(default=24, description="How far back to fetch")
    resource_group_filter: Optional[str] = Field(default=None, description="Filter by resource group")
    operation_name_filter: Optional[str] = Field(default=None, description="Filter by operation name")

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
            description=self.description or "Pull Azure Activity Log entries (control-plane audit) via the Monitor REST API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['azure', 'audit', 'monitor']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            from azure.identity import DefaultAzureCredential
            from azure.mgmt.monitor import MonitorManagementClient
            import datetime as dt
            sub_id = _self.subscription_id or os.environ["AZURE_SUBSCRIPTION_ID"]
            client = MonitorManagementClient(DefaultAzureCredential(), sub_id)
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(hours=_self.lookback_hours)
            f = f"eventTimestamp ge '{start.isoformat()}Z' and eventTimestamp le '{end.isoformat()}Z'"
            if _self.resource_group_filter:
                f += f" and resourceGroupName eq '{_self.resource_group_filter}'"
            if _self.operation_name_filter:
                f += f" and operationName eq '{_self.operation_name_filter}'"
            rows = []
            for ev in client.activity_logs.list(filter=f):
                rows.append({
                    "event_timestamp": ev.event_timestamp.isoformat() if ev.event_timestamp else None,
                    "operation_name": ev.operation_name.value if ev.operation_name else None,
                    "status": ev.status.value if ev.status else None,
                    "caller": ev.caller,
                    "resource_id": ev.resource_id,
                    "resource_group": ev.resource_group_name,
                    "level": ev.level,
                    "category": ev.category.value if ev.category else None,
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
