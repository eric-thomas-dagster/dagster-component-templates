"""VictoriaMetrics PromQL query → DataFrame asset.

Runs a PromQL query against VictoriaMetrics's Prometheus-compatible
``/api/v1/query_range`` endpoint and emits the result as a Pandas
DataFrame asset with columns ``timestamp``, ``value``, plus one column
per Prometheus label on the matched series.

For instant queries (single point), set ``query_range: false`` — uses
``/api/v1/query`` instead. The shape collapses to a single-row-per-
series DataFrame.

Docs: https://docs.victoriametrics.com/single-server-victoriametrics/#prometheus-querying-api-usage
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class VictoriaMetricsQueryAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a PromQL query against VictoriaMetrics; emit result as a DataFrame.

    Example:
        ```yaml
        type: dagster_community_components.VictoriaMetricsQueryAssetComponent
        attributes:
          asset_name: vm_request_rate_last_24h
          query: 'sum(rate(http_requests_total[5m])) by (service)'
          query_range: true
          range_lookback_minutes: 1440      # 24h
          step_seconds: 60
          base_url_env_var: VM_BASE_URL
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    query: str = Field(description="PromQL query string.")
    query_range: bool = Field(
        default=True,
        description="If true (default), use /api/v1/query_range; else /api/v1/query (instant).",
    )
    range_lookback_minutes: int = Field(
        default=60,
        ge=1,
        description="For query_range: lookback window from now (in minutes).",
    )
    step_seconds: int = Field(
        default=60,
        ge=1,
        description="For query_range: PromQL step (seconds between samples).",
    )

    base_url_env_var: str = Field(default="VM_BASE_URL", description="Env var with VictoriaMetrics base URL.")
    bearer_token_env_var: Optional[str] = Field(default=None, description="Env var with bearer token.")
    request_timeout_seconds: int = Field(default=60, ge=1)

    group_name: Optional[str] = Field(default="victoriametrics", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("victoriametrics")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"PromQL query against VictoriaMetrics → DataFrame: `{_self.query[:80]}`"
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            try:
                import requests
            except ImportError:
                raise ImportError("victoriametrics_query_asset requires `requests`")

            base_url = os.environ.get(_self.base_url_env_var, "")
            if not base_url:
                raise RuntimeError(f"Set {_self.base_url_env_var} to the VictoriaMetrics base URL.")
            headers = {}
            if _self.bearer_token_env_var and os.environ.get(_self.bearer_token_env_var):
                headers["Authorization"] = f"Bearer {os.environ[_self.bearer_token_env_var]}"

            if _self.query_range:
                end = datetime.now(timezone.utc)
                start = end - timedelta(minutes=_self.range_lookback_minutes)
                url = f"{base_url.rstrip('/')}/api/v1/query_range"
                params = {
                    "query": _self.query,
                    "start": int(start.timestamp()),
                    "end": int(end.timestamp()),
                    "step": _self.step_seconds,
                }
            else:
                url = f"{base_url.rstrip('/')}/api/v1/query"
                params = {"query": _self.query}

            context.log.info(f"VictoriaMetrics: GET {url} params={params}")
            resp = requests.get(url, params=params, headers=headers, timeout=_self.request_timeout_seconds)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("status") != "success":
                raise RuntimeError(f"VictoriaMetrics error: {payload.get('error', payload)}")
            result = payload.get("data", {}).get("result", [])

            # Normalize matrix (query_range) and vector (instant) result shapes
            # to columns: timestamp, value, plus one column per label.
            rows: List[Dict[str, Any]] = []
            for series in result:
                metric = series.get("metric", {}) or {}
                if _self.query_range:
                    for ts, val in series.get("values", []) or []:
                        rows.append({**metric,
                                     "timestamp": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                                     "value": float(val)})
                else:
                    ts, val = series.get("value", [None, None]) or [None, None]
                    if ts is not None:
                        rows.append({**metric,
                                     "timestamp": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                                     "value": float(val)})
            df = pd.DataFrame(rows)
            metadata = {
                "row_count": dg.MetadataValue.int(len(df)),
                "column_count": dg.MetadataValue.int(len(df.columns)),
                "vm/series_count": dg.MetadataValue.int(len(result)),
                "vm/query": _self.query,
                "vm/endpoint": dg.MetadataValue.url(url),
            }
            if len(df) > 0:
                preview = min(25, len(df))
                metadata["preview"] = dg.MetadataValue.md(df.head(preview).to_markdown(index=False))
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
