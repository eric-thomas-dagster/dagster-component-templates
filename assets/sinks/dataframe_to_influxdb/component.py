"""DataFrame → InfluxDB via line protocol.

Writes a Pandas DataFrame to InfluxDB 2.x / 3.x using the official
``influxdb_client.write_api`` with the line-protocol DataFrame writer.
Single configuration knob:

  - ``measurement:`` — name of the InfluxDB measurement
  - ``timestamp_column:`` — column with timestamps (default 'timestamp')
  - ``tag_columns:`` — columns to use as **tags** (indexed; low-cardinality)
  - ``field_columns:`` — columns to use as **fields** (values; numeric)

If ``tag_columns:`` / ``field_columns:`` are unset, the component
auto-classifies: numeric dtypes → fields, everything else → tags.

Pairs with ``influxdb_resource``.
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DataframeToInfluxDBComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-write a Pandas DataFrame to InfluxDB as line-protocol points.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToInfluxDBComponent
        attributes:
          asset_name: influxdb_metrics_write
          upstream_asset_key: service_throughput_per_region
          measurement: service_throughput
          bucket: metrics
          url_env_var: INFLUXDB_URL
          token_env_var: INFLUXDB_TOKEN
          org_env_var: INFLUXDB_ORG
          timestamp_column: ts
          tag_columns: [region, service]
          field_columns: [throughput, errors]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    measurement: str = Field(description="InfluxDB measurement name.")
    bucket: str = Field(description="Destination bucket.")
    timestamp_column: str = Field(default="timestamp", description="Column with datetime timestamps.")
    tag_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to use as tags (low-cardinality, indexed). Auto-derived if unset.",
    )
    field_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to use as fields (values, numeric). Auto-derived if unset.",
    )

    url_env_var: str = Field(default="INFLUXDB_URL", description="Env var with InfluxDB base URL.")
    token_env_var: str = Field(default="INFLUXDB_TOKEN", description="Env var with API token.")
    org_env_var: str = Field(default="INFLUXDB_ORG", description="Env var with organization name.")

    batch_size: int = Field(default=5000, ge=1, description="Rows per write batch.")
    request_timeout_ms: int = Field(default=30000, ge=100)

    group_name: Optional[str] = Field(default="influxdb", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("influxdb")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Write DataFrame to InfluxDB measurement `{_self.measurement}` in bucket `{_self.bucket}`."
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
                from influxdb_client import InfluxDBClient, WriteOptions
                from influxdb_client.client.write_api import SYNCHRONOUS  # type: ignore
            except ImportError:
                raise ImportError(
                    "dataframe_to_influxdb requires `influxdb-client`: "
                    "pip install influxdb-client"
                )

            url = os.environ.get(_self.url_env_var, "")
            token = os.environ.get(_self.token_env_var, "")
            org = os.environ.get(_self.org_env_var, "")
            if not url or not token or not org:
                raise RuntimeError(
                    f"InfluxDB env vars missing: {_self.url_env_var}, "
                    f"{_self.token_env_var}, {_self.org_env_var}"
                )

            ts_col = _self.timestamp_column
            if ts_col not in upstream.columns:
                raise RuntimeError(f"timestamp_column={ts_col!r} not in DataFrame columns")

            # Auto-classify columns when explicit lists not provided:
            # numeric → fields, others → tags. timestamp_column → index.
            if _self.tag_columns is None and _self.field_columns is None:
                tags = [c for c in upstream.columns
                        if c != ts_col and not pd.api.types.is_numeric_dtype(upstream[c])]
                fields = [c for c in upstream.columns
                          if c != ts_col and pd.api.types.is_numeric_dtype(upstream[c])]
            else:
                tags = list(_self.tag_columns or [])
                fields = list(_self.field_columns or [])

            df = upstream.copy()
            df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
            df = df.set_index(ts_col)

            written = 0
            with InfluxDBClient(url=url, token=token, org=org, timeout=_self.request_timeout_ms) as client:
                with client.write_api(write_options=SYNCHRONOUS) as write_api:
                    for start in range(0, len(df), _self.batch_size):
                        chunk = df.iloc[start : start + _self.batch_size]
                        write_api.write(
                            bucket=_self.bucket,
                            org=org,
                            record=chunk,
                            data_frame_measurement_name=_self.measurement,
                            data_frame_tag_columns=tags or None,
                        )
                        written += len(chunk)

            context.log.info(
                f"InfluxDB write: {written} points → {_self.measurement} in bucket {_self.bucket}"
            )
            return dg.MaterializeResult(metadata={
                "row_count": dg.MetadataValue.int(written),
                "influxdb_measurement": _self.measurement,
                "influxdb_bucket": _self.bucket,
                "influxdb/tags": dg.MetadataValue.json(tags),
                "influxdb/fields": dg.MetadataValue.json(fields),
            })

        return dg.Definitions(assets=[_asset])
