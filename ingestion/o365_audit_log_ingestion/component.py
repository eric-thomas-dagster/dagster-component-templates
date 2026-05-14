"""O365AuditLogIngestionComponent.

Pull Microsoft 365 / Office 365 Unified Audit Log entries via the Management Activity API.

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class O365AuditLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Microsoft 365 / Office 365 Unified Audit Log entries via the Management Activity API."""

    asset_name: str = Field(description="Dagster asset name")

    tenant_id: str = Field(description="Azure AD tenant ID")
    client_id: str = Field(description="App registration client ID")
    client_secret_env: str = Field(default="O365_CLIENT_SECRET", description="Env var holding client secret")
    content_type: str = Field(default="Audit.General", description="Audit.General | Audit.AzureActiveDirectory | Audit.Exchange | Audit.SharePoint | DLP.All")
    lookback_hours: int = Field(default=24, description="How far back")

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
            description=self.description or "Pull Microsoft 365 / Office 365 Unified Audit Log entries via the Management Activity API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['o365', 'audit', 'microsoft']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            import requests, datetime as dt
            secret = os.environ[_self.client_secret_env]
            # 1. Get token
            token_url = f"https://login.microsoftonline.com/{_self.tenant_id}/oauth2/v2.0/token"
            tr = requests.post(token_url, data={
                "client_id": _self.client_id, "client_secret": secret,
                "grant_type": "client_credentials",
                "scope": "https://manage.office.com/.default",
            }, timeout=60)
            tr.raise_for_status()
            access = tr.json()["access_token"]
            headers = {"Authorization": f"Bearer {access}"}
            # 2. List content blobs
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(hours=_self.lookback_hours)
            list_url = f"https://manage.office.com/api/v1.0/{_self.tenant_id}/activity/feed/subscriptions/content"
            r = requests.get(list_url, params={
                "contentType": _self.content_type,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
            }, headers=headers, timeout=60)
            r.raise_for_status()
            blobs = r.json()
            # 3. Fetch each blob
            all_events = []
            for b in blobs:
                br = requests.get(b["contentUri"], headers=headers, timeout=60)
                br.raise_for_status()
                all_events.extend(br.json())
            df = pd.DataFrame(all_events)
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
