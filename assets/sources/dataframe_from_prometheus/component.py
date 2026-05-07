"""Prometheus → DataFrame (PromQL query source).

Run a PromQL query against any Prometheus-compatible server's HTTP API
(/api/v1/query for instant, /api/v1/query_range for range) and
materialize the result as a DataFrame asset.

Works against:
- Open-source Prometheus
- Cortex / Thanos / Mimir
- VictoriaMetrics
- **Azure Managed Prometheus** (set bearer_token_env_var to a token
  from `az account get-access-token --resource <prometheus-endpoint>`)
- Grafana Cloud
- AWS Managed Prometheus (with sigv4 sidecar)

For PUSHING metrics, see the companion `dataframe_to_prometheus` sink
or the existing `prometheus_resource` (pushgateway).
"""

import os
from datetime import datetime, timedelta
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


class DataframeFromPrometheusComponent(Component, Model, Resolvable):
    """Run a PromQL query and materialize the result as a DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")
    server_url: str = Field(
        description=(
            "Prometheus base URL (no trailing slash), e.g. "
            "'https://prometheus.demo.do.prometheus.io', "
            "'https://my-managed-prometheus.eastus.prometheus.monitor.azure.com', "
            "'http://localhost:9090'"
        )
    )
    query: str = Field(
        description="PromQL query, e.g. 'rate(http_requests_total[5m])' or 'up{job=\"node\"}'"
    )

    range_query: bool = Field(
        default=False,
        description=(
            "False (default): instant query at time 'now' (single value per series). "
            "True: range query (returns a time series per matching series)."
        ),
    )
    range_seconds: int = Field(
        default=3600,
        description="Range duration in seconds (only used when range_query=true). Default: last hour.",
    )
    step_seconds: int = Field(
        default=60,
        description="Step (sample interval) in seconds for range queries. Default: 60s.",
    )

    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a Bearer token for Authorization header. Required for "
            "Azure Managed Prometheus, Grafana Cloud, and any auth-protected server. "
            "For Azure Managed Prometheus: az account get-access-token "
            "--resource https://prometheus.monitor.azure.com"
        ),
    )
    basic_auth_user_env_var: Optional[str] = Field(default=None)
    basic_auth_pass_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional headers (e.g. X-Scope-OrgID for Cortex/Mimir tenants)",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["prometheus", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"PromQL query against {cfg.server_url}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def prometheus_source(context: AssetExecutionContext) -> MaterializeResult:
            import requests

            headers = {}
            if cfg.bearer_token_env_var:
                token = os.environ[cfg.bearer_token_env_var]
                headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            auth = None
            if cfg.basic_auth_user_env_var and cfg.basic_auth_pass_env_var:
                auth = (
                    os.environ[cfg.basic_auth_user_env_var],
                    os.environ[cfg.basic_auth_pass_env_var],
                )

            base = cfg.server_url.rstrip("/")
            if cfg.range_query:
                end = datetime.utcnow()
                start = end - timedelta(seconds=cfg.range_seconds)
                params = {
                    "query": cfg.query,
                    "start": start.timestamp(),
                    "end": end.timestamp(),
                    "step": cfg.step_seconds,
                }
                resp = requests.get(f"{base}/api/v1/query_range", params=params, headers=headers, auth=auth, timeout=60)
            else:
                resp = requests.get(f"{base}/api/v1/query", params={"query": cfg.query}, headers=headers, auth=auth, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                raise Exception(f"PromQL error: {data.get('error', 'unknown')}")

            result_type = data["data"]["resultType"]
            results = data["data"]["result"]

            rows = []
            for series in results:
                metric = series.get("metric", {})
                if result_type == "matrix":
                    for ts, val in series.get("values", []):
                        rows.append({**metric, "timestamp": pd.to_datetime(float(ts), unit="s"), "value": float(val)})
                elif result_type == "vector":
                    ts, val = series.get("value", [None, None])
                    if ts is not None:
                        rows.append({**metric, "timestamp": pd.to_datetime(float(ts), unit="s"), "value": float(val)})
                elif result_type in ("scalar", "string"):
                    ts, val = series if isinstance(series, list) else (None, None)
                    if ts is not None:
                        rows.append({"timestamp": pd.to_datetime(float(ts), unit="s"), "value": val})

            df = pd.DataFrame(rows)
            context.log.info(f"PromQL returned {len(df)} samples ({result_type})")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "result_type": MetadataValue.text(result_type),
                    "server": MetadataValue.text(cfg.server_url),
                    "query": MetadataValue.text(cfg.query),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[prometheus_source])
