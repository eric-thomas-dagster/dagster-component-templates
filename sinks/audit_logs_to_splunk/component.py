"""AuditLogsToSplunkComponent.

Ship audit-log DataFrame to Splunk via the HTTP Event Collector (HEC).

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToSplunkComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Splunk via the HTTP Event Collector (HEC)."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    hec_url: str = Field(description="Splunk HEC endpoint (e.g. 'https://splunk.acme.com:8088/services/collector')")
    hec_token_env: str = Field(default="SPLUNK_HEC_TOKEN", description="Env var with HEC token")
    index: Optional[str] = Field(default=None, description="Splunk index (overrides token's default)")
    sourcetype: Optional[str] = Field(default=None, description="sourcetype")
    source: Optional[str] = Field(default=None, description="source")
    host: Optional[str] = Field(default=None, description="host field")
    batch_size: int = Field(default=500, description="Events per HTTP POST")
    verify_ssl: bool = Field(default=True, description="Verify TLS cert (set false for self-signed dev splunk)")

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
            description=self.description or "Ship audit-log DataFrame to Splunk via the HTTP Event Collector (HEC).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['splunk', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            import requests
            token = os.environ[_self.hec_token_env]
            sent, errors = 0, []
            for i in range(0, len(df), _self.batch_size):
                chunk = df.iloc[i : i + _self.batch_size]
                payload = "\n".join(
                    json.dumps({
                        "event": json.loads(row.to_json()),
                        **({"index": _self.index} if _self.index else {}),
                        **({"sourcetype": _self.sourcetype} if _self.sourcetype else {}),
                        **({"source": _self.source} if _self.source else {}),
                        **({"host": _self.host} if _self.host else {}),
                    })
                    for _, row in chunk.iterrows()
                )
                r = requests.post(_self.hec_url, data=payload,
                                  headers={"Authorization": f"Splunk {token}"},
                                  verify=_self.verify_ssl, timeout=60)
                if r.status_code >= 300:
                    errors.append(f"batch {i}: {r.status_code} {r.text[:200]}")
                else:
                    sent += len(chunk)
            if errors:
                raise Exception(f"Splunk HEC errors: {errors}")
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "hec_url": dg.MetadataValue.text(_self.hec_url),
                "index": dg.MetadataValue.text(_self.index or "(token default)"),
            })

        return dg.Definitions(assets=[_asset])
