"""DataFrame → Dynatrace Events.

Push DataFrame rows as generic Dynatrace events via the v2 Events API.
Use this for batch-job markers (deployments, ETL completions, anomalies)
that should annotate Dynatrace's timeline alongside operational metrics.
"""

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


class DataframeToDynatraceEventsComponent(Component, Model, Resolvable):
    """Ship DataFrame rows as Dynatrace v2 events."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    environment_url: str = Field(description="Dynatrace environment URL")
    api_token_env_var: str = Field(description="Env var holding the API token (events.ingest scope)")

    event_type: str = Field(
        default="CUSTOM_INFO",
        description="Dynatrace event type: 'CUSTOM_INFO' | 'CUSTOM_DEPLOYMENT' | 'CUSTOM_ANNOTATION' | 'AVAILABILITY_EVENT' | 'ERROR_EVENT' | 'PERFORMANCE_EVENT' | 'RESOURCE_CONTENTION_EVENT'",
    )
    title_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the event title. Default: synthesized from row.",
    )
    entity_selector: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dynatrace entity selector to attach events to. "
            "Example: 'type(HOST),tag(\"env:prod\")'. Required if you want events on a specific entity."
        ),
    )
    properties_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns whose values become custom event properties. Default: all columns.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["dynatrace", "events", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Push DataFrame rows as Dynatrace events",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def dynatrace_events(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            import requests
            token = os.environ.get(cfg.api_token_env_var)
            if not token:
                raise RuntimeError(f"Missing {cfg.api_token_env_var}")
            url = f"{cfg.environment_url.rstrip('/')}/api/v2/events/ingest"

            sent, failed = 0, 0
            records = upstream.to_dict(orient="records")
            prop_cols = cfg.properties_columns or list(upstream.columns)

            for r in records:
                title = str(r.get(cfg.title_column)) if cfg.title_column else f"Dagster event: {r.get(prop_cols[0]) if prop_cols else 'row'}"
                payload = {
                    "eventType": cfg.event_type,
                    "title": title,
                    "properties": {
                        c: str(r[c]) for c in prop_cols if c in r and r[c] is not None and not pd.isna(r[c])
                    },
                }
                if cfg.entity_selector:
                    payload["entitySelector"] = cfg.entity_selector
                resp = requests.post(
                    url,
                    json=payload,
                    headers={"Authorization": f"Api-Token {token}", "Content-Type": "application/json"},
                    timeout=30,
                )
                if 200 <= resp.status_code < 300:
                    sent += 1
                else:
                    failed += 1
                    context.log.warning(f"Dynatrace event failed: {resp.status_code} {resp.text[:200]}")

            context.log.info(f"Pushed {sent}/{len(records)} events to Dynatrace")
            return MaterializeResult(
                metadata={
                    "events_sent": MetadataValue.int(sent),
                    "events_failed": MetadataValue.int(failed),
                    "event_type": MetadataValue.text(cfg.event_type),
                    "environment_url": MetadataValue.text(cfg.environment_url),
                }
            )

        return Definitions(assets=[dynatrace_events])
