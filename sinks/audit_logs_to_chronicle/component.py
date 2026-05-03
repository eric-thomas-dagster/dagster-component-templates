"""AuditLogsToChronicleComponent.

Ship audit-log DataFrame to Google Chronicle (SecOps) via the unstructuredlogentries ingestion API.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToChronicleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Google Chronicle (SecOps) via the unstructuredlogentries ingestion API."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    customer_id: str = Field(description="Chronicle customer ID (GUID)")
    region: str = Field(default="us", description="Chronicle region: 'us', 'eu', 'asia-southeast1', etc.")
    sa_credentials_path: str = Field(description="Path to GCP service account JSON with chronicle scope")
    log_type: str = Field(default="WORKDAY", description="Chronicle log type (WORKDAY, OKTA, AWS_CLOUDTRAIL, etc.)")
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
            description=self.description or "Ship audit-log DataFrame to Google Chronicle (SecOps) via the unstructuredlogentries ingestion API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['chronicle', 'google', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            from google.oauth2 import service_account
            import google.auth.transport.requests
            import requests
            creds = service_account.Credentials.from_service_account_file(
                _self.sa_credentials_path,
                scopes=["https://www.googleapis.com/auth/malachite-ingestion"],
            )
            creds.refresh(google.auth.transport.requests.Request())
            url = f"https://malachiteingestion-pa.googleapis.com/v2/unstructuredlogentries:batchCreate"
            sent = 0
            for i in range(0, len(df), _self.batch_size):
                chunk = df.iloc[i : i + _self.batch_size]
                entries = [{"logText": json.dumps(json.loads(row.to_json()))} for _, row in chunk.iterrows()]
                payload = {
                    "customer_id": _self.customer_id,
                    "log_type": _self.log_type,
                    "entries": entries,
                }
                r = requests.post(url, json=payload, headers={"Authorization": f"Bearer {creds.token}"}, timeout=60)
                if r.status_code >= 300:
                    raise Exception(f"Chronicle error: {r.status_code} {r.text[:200]}")
                sent += len(chunk)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "log_type": dg.MetadataValue.text(_self.log_type),
            })

        return dg.Definitions(assets=[_asset])
