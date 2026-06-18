"""DataFrame → StarRocks via Stream Load.

StarRocks is the literal open-source fork of Apache Doris — same MySQL
wire protocol, same **Stream Load** HTTP API. This component mirrors
``dataframe_to_doris`` 1:1 because the Stream Load contract is
identical: PUT serialized DataFrame (CSV or JSON) to
``http://{fe}:{http_port}/api/{db}/{table}/_stream_load`` with the
same headers + response shape.

Why a separate component instead of reusing ``dataframe_to_doris``?
Discoverability — customers searching "starrocks" find this directly,
manifest entries surface under the StarRocks vendor in the UI, and
README/walkthroughs can reference StarRocks-specific knobs as they
diverge over time.

Docs: https://docs.starrocks.io/en-us/main/loading/StreamLoad
"""
import io
import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


class DataframeToStarRocksComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-ingest a Pandas DataFrame into StarRocks via Stream Load.

    Mirrors ``dataframe_to_doris`` — same wire protocol, same response
    shape. Use this when your tables live on StarRocks instead of Doris.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToStarRocksComponent
        attributes:
          asset_name: starrocks_orders_load
          upstream_asset_key: orders_clean
          table: orders
          database: analytics
          host_env_var: STARROCKS_FE_HOST
          username_env_var: STARROCKS_USER
          password_env_var: STARROCKS_PASSWORD
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    table: str = Field(description="Destination StarRocks table name.")
    database: str = Field(description="Destination StarRocks database.")

    host_env_var: str = Field(default="STARROCKS_FE_HOST", description="Env var with StarRocks FE host.")
    http_port: int = Field(default=8030, description="StarRocks FE HTTP port.")
    username_env_var: str = Field(default="STARROCKS_USER", description="Env var with StarRocks username.")
    password_env_var: str = Field(default="STARROCKS_PASSWORD", description="Env var with StarRocks password.")
    ssl: bool = Field(default=False, description="Use https for the Stream Load endpoint.")

    format: str = Field(default="csv", description="'csv' or 'json'.")
    column_separator: Union[str, int] = Field(default=",", description="CSV column separator.")
    line_delimiter: str = Field(default="\\n", description="CSV line delimiter.")
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Optional explicit column list (defaults to DataFrame column order).",
    )
    request_timeout_seconds: int = Field(default=300, ge=10)

    group_name: Optional[str] = Field(default="starrocks", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'starrocks').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("starrocks")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Bulk-load DataFrame into StarRocks table {_self.database}.{_self.table} via Stream Load."
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
                raise ImportError("dataframe_to_starrocks requires `requests`: pip install requests")

            host = os.environ.get(_self.host_env_var, "")
            user = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var, "")
            if not host or not user:
                raise RuntimeError(f"StarRocks creds missing: set {_self.host_env_var} + {_self.username_env_var}.")

            scheme = "https" if _self.ssl else "http"
            url = f"{scheme}://{host}:{_self.http_port}/api/{_self.database}/{_self.table}/_stream_load"

            buf = io.BytesIO()
            if _self.format == "csv":
                upstream.to_csv(
                    buf, sep=_self.column_separator, index=False, header=False,
                    lineterminator=_self.line_delimiter,
                )
                data = buf.getvalue()
                headers = {
                    "format": "csv",
                    "column_separator": _self.column_separator,
                    "line_delimiter": _self.line_delimiter,
                    "Expect": "100-continue",
                    "label": f"dagster_{context.run.run_id}_{_self.table}",
                }
            elif _self.format == "json":
                upstream.to_json(buf, orient="records", lines=False)
                data = buf.getvalue()
                headers = {
                    "format": "json",
                    "strip_outer_array": "true",
                    "Expect": "100-continue",
                    "label": f"dagster_{context.run.run_id}_{_self.table}",
                }
            else:
                raise ValueError(f"format must be 'csv' or 'json'; got {_self.format!r}.")

            if _self.columns:
                headers["columns"] = ",".join(_self.columns)
            elif _self.format == "csv":
                headers["columns"] = ",".join(str(c) for c in upstream.columns)

            context.log.info(f"StarRocks Stream Load: PUT {url} ({len(upstream)} rows, {_self.format})")
            resp = requests.put(
                url, data=data, headers=headers, auth=(user, password),
                timeout=_self.request_timeout_seconds, allow_redirects=True,
            )
            resp.raise_for_status()
            result = resp.json()

            status = result.get("Status", "Unknown")
            metadata: Dict[str, Any] = {
                "starrocks_status": status,
                "starrocks/loaded_rows": dg.MetadataValue.int(int(result.get("NumberLoadedRows", 0))),
                "starrocks/filtered_rows": dg.MetadataValue.int(int(result.get("NumberFilteredRows", 0))),
                "starrocks/total_rows": dg.MetadataValue.int(int(result.get("NumberTotalRows", 0))),
                "starrocks/load_time_ms": dg.MetadataValue.int(int(result.get("LoadTimeMs", 0))),
                "starrocks_table": f"{_self.database}.{_self.table}",
            }
            if result.get("ErrorURL"):
                metadata["starrocks_error_url"] = dg.MetadataValue.url(result["ErrorURL"])
            if status != "Success":
                raise RuntimeError(
                    f"StarRocks Stream Load returned {status}: "
                    f"{result.get('Message', '<no message>')}. "
                    f"ErrorURL: {result.get('ErrorURL', '<none>')}"
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
