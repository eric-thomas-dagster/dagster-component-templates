"""DataFrame → VictoriaMetrics via Prometheus text format.

Ingests a Pandas DataFrame as VictoriaMetrics time-series via the
``/api/v1/import/prometheus`` endpoint. DataFrame schema:

  - **timestamp** (datetime, required) — series sample time
  - **value** (float, required) — sample value
  - Every other column is treated as a LABEL on the metric

Plus a top-level ``metric_name:`` field on the component that names the
metric (e.g. ``app_requests_total``).

Why Prometheus text format and not remote-write? The text format is
single-character-escaping simple, doesn't require snappy + protobuf
buffers, and matches what curl-able exports look like. For very high
throughput (>1M samples/sec sustained), remote-write is more efficient
— file an issue if you need it; would add ``format: text|remote_write``.

Docs: https://docs.victoriametrics.com/single-server-victoriametrics/#how-to-import-data-in-prometheus-exposition-format
"""
import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


def _escape_label_value(v: str) -> str:
    """Prometheus label-value quoting: backslash, double-quote, newline."""
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class DataframeToVictoriaMetricsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-ingest a Pandas DataFrame as VictoriaMetrics time-series.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToVictoriaMetricsComponent
        attributes:
          asset_name: vm_orders_throughput
          upstream_asset_key: orders_throughput_per_region
          metric_name: orders_throughput
          base_url_env_var: VM_BASE_URL
          timestamp_column: ts
          value_column: throughput
          # All other columns become labels:
          # (region, source) on every emitted series
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    metric_name: str = Field(description="Prometheus metric name (e.g. 'orders_throughput').")
    timestamp_column: Union[str, int] = Field(default="timestamp", description="Column with datetime timestamps.")
    value_column: Union[str, int] = Field(default="value", description="Column with numeric metric values.")
    label_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Explicit column list to use as labels. If unset, every column "
            "other than timestamp_column + value_column becomes a label."
        ),
    )

    base_url_env_var: str = Field(default="VM_BASE_URL", description="Env var with VictoriaMetrics base URL.")
    bearer_token_env_var: Optional[str] = Field(default=None, description="Env var with bearer token.")
    request_timeout_seconds: int = Field(default=60, ge=1)

    group_name: Optional[str] = Field(default="victoriametrics", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("victoriametrics")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Bulk-ingest DataFrame as VictoriaMetrics metric "
                f"`{_self.metric_name}` via Prometheus text format."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream: Any) -> dg.MaterializeResult:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                import requests
            except ImportError:
                raise ImportError("dataframe_to_victoriametrics requires `requests`: pip install requests")

            base_url = os.environ.get(_self.base_url_env_var, "")
            if not base_url:
                raise RuntimeError(f"Set {_self.base_url_env_var} to the VictoriaMetrics base URL.")
            url = f"{base_url.rstrip('/')}/api/v1/import/prometheus"
            headers = {"Content-Type": "text/plain; charset=utf-8"}
            if _self.bearer_token_env_var and os.environ.get(_self.bearer_token_env_var):
                headers["Authorization"] = f"Bearer {os.environ[_self.bearer_token_env_var]}"

            ts_col = _self.timestamp_column
            val_col = _self.value_column
            if ts_col not in upstream.columns:
                raise RuntimeError(f"timestamp_column={ts_col!r} not in DataFrame columns")
            if val_col not in upstream.columns:
                raise RuntimeError(f"value_column={val_col!r} not in DataFrame columns")

            label_cols = _self.label_columns or [c for c in upstream.columns if c not in (ts_col, val_col)]

            # Serialize to Prometheus text format:
            # metric_name{label1="v1",label2="v2"} value timestamp_ms\n
            lines: List[str] = []
            for _, row in upstream.iterrows():
                labels: List[str] = []
                for col in label_cols:
                    v = row[col]
                    if pd.isna(v):
                        continue
                    labels.append(f'{col}="{_escape_label_value(str(v))}"')
                label_str = "{" + ",".join(labels) + "}" if labels else ""
                ts = pd.to_datetime(row[ts_col])
                ts_ms = int(ts.timestamp() * 1000)
                val = float(row[val_col])
                lines.append(f"{_self.metric_name}{label_str} {val} {ts_ms}")
            body = "\n".join(lines) + "\n"

            context.log.info(
                f"VictoriaMetrics ingest: {len(lines)} samples → {_self.metric_name} ({len(label_cols)} labels)"
            )
            resp = requests.post(
                url, data=body.encode("utf-8"),
                headers=headers, timeout=_self.request_timeout_seconds,
            )
            resp.raise_for_status()

            return dg.MaterializeResult(metadata={
                "row_count": dg.MetadataValue.int(len(upstream)),
                "samples_written": dg.MetadataValue.int(len(lines)),
                "vm/metric_name": _self.metric_name,
                "vm/label_columns": dg.MetadataValue.json(label_cols),
                "vm/endpoint": dg.MetadataValue.url(url),
            })

        return dg.Definitions(assets=[_asset])
