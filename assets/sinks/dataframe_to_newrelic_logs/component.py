"""DataFrame → New Relic Logs.

Push DataFrame rows as log events to New Relic. Each row becomes one
log entry; the entire row is sent as JSON attributes.

API: POST https://log-api.newrelic.com/log/v1 (US) or
     POST https://log-api.eu.newrelic.com/log/v1 (EU)

Auth: License Key OR User API Key in the `Api-Key` header.
"""

import json
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


class DataframeToNewRelicLogsComponent(Component, Model, Resolvable):
    """Ship a DataFrame as log events to New Relic."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    api_key_env_var: str = Field(description="Env var holding the License Key or User API Key")
    region: str = Field(default="US", description="'US' or 'EU'")

    log_type: Optional[str] = Field(
        default=None,
        description="Optional logtype field to set on every event (used for downstream parsing rules)",
    )
    timestamp_column: Optional[str] = Field(
        default=None,
        description="Column with epoch-ms timestamps (default: now() per event)",
    )
    message_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the log 'message' field. If unset, JSON-stringifies the whole row.",
    )
    batch_size: int = Field(default=1000, description="Max events per HTTP POST")

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["newrelic", "logs", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Ship rows as log events to New Relic",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def newrelic_logs(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            import requests
            api_key = os.environ.get(cfg.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"Missing {cfg.api_key_env_var}")
            url = (
                "https://log-api.newrelic.com/log/v1"
                if cfg.region.upper() == "US"
                else "https://log-api.eu.newrelic.com/log/v1"
            )

            records = upstream.to_dict(orient="records")
            sent, failed = 0, 0
            for chunk_start in range(0, len(records), cfg.batch_size):
                chunk = records[chunk_start : chunk_start + cfg.batch_size]
                events = []
                for r in chunk:
                    msg = (
                        str(r.get(cfg.message_column))
                        if cfg.message_column
                        else json.dumps(r, default=str)
                    )
                    e = {**{k: (v if not pd.isna(v) else None) for k, v in r.items()}, "message": msg}
                    if cfg.log_type:
                        e["logtype"] = cfg.log_type
                    if cfg.timestamp_column and cfg.timestamp_column in r:
                        e["timestamp"] = int(r[cfg.timestamp_column])
                    events.append(e)
                resp = requests.post(
                    url,
                    json=events,
                    headers={"Api-Key": api_key, "Content-Type": "application/json"},
                    timeout=60,
                )
                if 200 <= resp.status_code < 300:
                    sent += len(chunk)
                else:
                    failed += len(chunk)
                    context.log.warning(f"NR logs push failed: {resp.status_code} {resp.text[:200]}")

            context.log.info(f"Pushed {sent}/{len(records)} log events to New Relic ({cfg.region})")
            return MaterializeResult(
                metadata={
                    "events_sent": MetadataValue.int(sent),
                    "events_failed": MetadataValue.int(failed),
                    "region": MetadataValue.text(cfg.region),
                    "url": MetadataValue.text(url),
                }
            )

        return Definitions(assets=[newrelic_logs])
