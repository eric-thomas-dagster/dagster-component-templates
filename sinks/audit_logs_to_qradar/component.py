"""AuditLogsToQradarComponent.

Ship audit-log DataFrame to IBM QRadar via Syslog (TCP) — events go to a configured Log Source.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToQradarComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to IBM QRadar via Syslog (TCP) — events go to a configured Log Source."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    qradar_host: str = Field(description="QRadar event collector host (or LEEF endpoint)")
    syslog_port: int = Field(default=514, description="Syslog TCP port")
    facility: int = Field(default=16, description="Syslog facility (16 = local0)")
    severity: int = Field(default=6, description="Syslog severity (6 = informational)")
    leef_format: bool = Field(default=False, description="Wrap events in LEEF 2.0 format")
    leef_vendor: str = Field(default="Dagster")
    leef_product: str = Field(default="Audit")
    leef_version: str = Field(default="1.0")

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
            description=self.description or "Ship audit-log DataFrame to IBM QRadar via Syslog (TCP) — events go to a configured Log Source.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['qradar', 'ibm', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            import socket, datetime as dt
            sock = socket.create_connection((_self.qradar_host, _self.syslog_port), timeout=30)
            try:
                sent = 0
                for _, row in df.iterrows():
                    event_json = row.to_json()
                    ts = dt.datetime.utcnow().strftime("%b %d %H:%M:%S")
                    priority = _self.facility * 8 + _self.severity
                    if _self.leef_format:
                        leef = f"LEEF:2.0|{_self.leef_vendor}|{_self.leef_product}|{_self.leef_version}|audit|{event_json}"
                        line = f"<{priority}>{ts} dagster {leef}\n"
                    else:
                        line = f"<{priority}>{ts} dagster audit: {event_json}\n"
                    sock.sendall(line.encode())
                    sent += 1
            finally:
                sock.close()
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(sent),
                "qradar_host": dg.MetadataValue.text(f"{_self.qradar_host}:{_self.syslog_port}"),
            })

        return dg.Definitions(assets=[_asset])
