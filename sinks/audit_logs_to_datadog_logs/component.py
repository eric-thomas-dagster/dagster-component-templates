"""AuditLogsToDatadogLogsComponent.

Ship audit-log DataFrame to Datadog Logs via /api/v2/logs.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToDatadogLogsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Datadog Logs via /api/v2/logs."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    api_key_env: str = Field(default="DD_API_KEY", description="Env var with Datadog API key")
    site: str = Field(default="datadoghq.com", description="datadoghq.com | datadoghq.eu | us3.datadoghq.com | ...")
    service: str = Field(default="dagster-audit", description="Datadog service tag")
    ddsource: str = Field(default="dagster", description="ddsource attribute")
    ddtags: Optional[str] = Field(default=None, description="Comma-separated tags (env:prod,team:security)")
    batch_size: int = Field(default=1000)

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_sink", description="Dagster asset group")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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
            description=self.description or "Ship audit-log DataFrame to Datadog Logs via /api/v2/logs.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['datadog', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            import requests
            api_key = os.environ[_self.api_key_env]
            url = f"https://http-intake.logs.{_self.site}/api/v2/logs"
            sent = 0
            for i in range(0, len(df), _self.batch_size):
                chunk = df.iloc[i : i + _self.batch_size]
                payload = []
                for _, row in chunk.iterrows():
                    e = {"message": row.to_json(), "service": _self.service, "ddsource": _self.ddsource}
                    if _self.ddtags:
                        e["ddtags"] = _self.ddtags
                    payload.append(e)
                r = requests.post(url, json=payload, headers={"DD-API-KEY": api_key}, timeout=60)
                if r.status_code >= 300:
                    raise Exception(f"Datadog error: {r.status_code} {r.text[:200]}")
                sent += len(chunk)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "service": dg.MetadataValue.text(_self.service),
            })

        return dg.Definitions(assets=[_asset])
