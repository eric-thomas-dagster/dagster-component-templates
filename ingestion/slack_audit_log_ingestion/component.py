"""SlackAuditLogIngestionComponent.

Pull Slack Enterprise Grid audit log via /audit/v1/logs (requires audit_logs:read scope).

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class SlackAuditLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Slack Enterprise Grid audit log via /audit/v1/logs (requires audit_logs:read scope)."""

    asset_name: str = Field(description="Dagster asset name")

    token_env: str = Field(default="SLACK_AUDIT_TOKEN", description="Env var holding Slack Enterprise audit-log token")
    lookback_hours: int = Field(default=24, description="How far back")
    action_filter: Optional[str] = Field(default=None, description="Filter by action (e.g. 'user_login_failed')")
    limit: int = Field(default=200, description="Per-page (max 9999)")

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
            description=self.description or "Pull Slack Enterprise Grid audit log via /audit/v1/logs (requires audit_logs:read scope).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['slack', 'audit']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            import requests, datetime as dt
            token = os.environ[_self.token_env]
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(hours=_self.lookback_hours)
            url = "https://api.slack.com/audit/v1/logs"
            params = {"oldest": int(start.timestamp()), "latest": int(end.timestamp()), "limit": _self.limit}
            if _self.action_filter:
                params["action"] = _self.action_filter
            all_events, cursor = [], None
            while True:
                if cursor:
                    params["cursor"] = cursor
                r = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=60)
                r.raise_for_status()
                data = r.json()
                all_events.extend(data.get("entries", []))
                cursor = data.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
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
