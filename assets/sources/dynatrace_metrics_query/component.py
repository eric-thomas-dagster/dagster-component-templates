"""Dynatrace Metrics → DataFrame.

Run a metrics query via Dynatrace Metrics API v2 and materialize the
result as a DataFrame asset. Each metric series → rows of (timestamp,
value, dimensions...).
"""

import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
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


class DynatraceMetricsQueryComponent(Component, Model, Resolvable):
    """Query Dynatrace metrics → DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    environment_url: str = Field(description="Dynatrace environment URL")
    api_token_env_var: str = Field(description="Env var holding the API token (metrics.read scope)")

    metric_selector: str = Field(
        description=(
            "Dynatrace metric selector. Example: "
            "'builtin:host.cpu.usage:splitBy(dt.entity.host):avg' or "
            "'builtin:tech.generic.cpu.usage:filter(eq(dt.entity.host,HOST-XXXX))'"
        )
    )
    resolution: str = Field(
        default="1m",
        description="Resolution: '1m', '5m', '1h', etc. Coarser resolutions cover longer time-frames.",
    )
    timeframe: str = Field(
        default="now-1h",
        description="Time range: relative (now-Nh / now-Nd) or absolute ISO 8601 (start_iso/end_iso)",
    )
    entity_selector: Optional[str] = Field(
        default=None,
        description="Optional entity scope filter, e.g. 'type(HOST),tag(\"env:prod\")'",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["dynatrace", "metrics", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Dynatrace metric: {cfg.metric_selector[:80]}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def dynatrace_query(context: AssetExecutionContext) -> MaterializeResult:
            import requests
            token = os.environ.get(cfg.api_token_env_var)
            if not token:
                raise RuntimeError(f"Missing {cfg.api_token_env_var}")
            url = f"{cfg.environment_url.rstrip('/')}/api/v2/metrics/query"

            params = {
                "metricSelector": cfg.metric_selector,
                "resolution": cfg.resolution,
                "from": cfg.timeframe,
            }
            if cfg.entity_selector:
                params["entitySelector"] = cfg.entity_selector

            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Api-Token {token}"},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            rows = []
            for result in data.get("result", []):
                metric_id = result.get("metricId", cfg.metric_selector)
                for series in result.get("data", []):
                    dims = series.get("dimensionMap", {}) or {}
                    timestamps = series.get("timestamps", [])
                    values = series.get("values", [])
                    for ts, v in zip(timestamps, values):
                        rows.append({
                            **dims,
                            "metric_id": metric_id,
                            "timestamp": pd.to_datetime(int(ts), unit="ms"),
                            "value": v,
                        })
            df = pd.DataFrame(rows)
            context.log.info(f"Dynatrace returned {len(df)} samples")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "metric_selector": MetadataValue.text(cfg.metric_selector),
                    "timeframe": MetadataValue.text(cfg.timeframe),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[dynatrace_query])
