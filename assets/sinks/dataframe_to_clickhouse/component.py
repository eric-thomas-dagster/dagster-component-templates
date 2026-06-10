"""DataFrame → ClickHouse via clickhouse-connect.

Bulk-insert a Pandas DataFrame into a ClickHouse table using the
official ``clickhouse-connect`` client. clickhouse-connect's
``client.insert_df()`` is the most efficient path for Pandas →
ClickHouse — it batches rows + uses the binary RowBinary format under
HTTP.

For very large DataFrames (>10M rows), prefer running multiple
materializations via Dagster partitions over one giant single-asset run.
clickhouse-connect handles ~1M rows/sec on a single client; partitioned
ingestion scales linearly.

Pairs with:
  - ``clickhouse_resource`` — shared connection
  - ``external_clickhouse_table`` — declare-only catalog entry for
    tables ClickHouse owns (e.g. Materialized Views, Distributed)

Docs: https://clickhouse.com/docs/en/integrations/python
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DataframeToClickHouseComponent(dg.Component, dg.Model, dg.Resolvable):
    """Insert a Pandas DataFrame into a ClickHouse table.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToClickHouseComponent
        attributes:
          asset_name: clickhouse_events_load
          upstream_asset_key: events_clean
          table: events
          database: analytics
          host_env_var: CLICKHOUSE_HOST
          port: 8443
          secure: true
          username_env_var: CLICKHOUSE_USER
          password_env_var: CLICKHOUSE_PASSWORD
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    table: str = Field(description="Destination ClickHouse table name.")
    database: str = Field(default="default", description="Destination ClickHouse database.")

    host_env_var: str = Field(default="CLICKHOUSE_HOST", description="Env var with ClickHouse hostname.")
    port: int = Field(default=8123, description="ClickHouse HTTP port (8123 plain / 8443 TLS).")
    username_env_var: str = Field(default="CLICKHOUSE_USER", description="Env var with username.")
    password_env_var: str = Field(default="CLICKHOUSE_PASSWORD", description="Env var with password.")
    secure: bool = Field(default=False, description="Enable TLS.")

    column_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional explicit column list (must match destination table order). "
            "If unset, DataFrame column order is used."
        ),
    )
    request_timeout_seconds: int = Field(default=300, ge=10)

    group_name: Optional[str] = Field(default="clickhouse", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'clickhouse').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("clickhouse")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Insert DataFrame into ClickHouse {_self.database}.{_self.table} via clickhouse-connect."
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
                import clickhouse_connect
            except ImportError:
                raise ImportError(
                    "dataframe_to_clickhouse requires `clickhouse-connect`: "
                    "pip install clickhouse-connect"
                )

            host = os.environ.get(_self.host_env_var, "")
            user = os.environ.get(_self.username_env_var, "default")
            password = os.environ.get(_self.password_env_var, "")
            if not host:
                raise RuntimeError(f"ClickHouse host missing: set {_self.host_env_var}.")

            client = clickhouse_connect.get_client(
                host=host,
                port=_self.port,
                username=user,
                password=password,
                database=_self.database,
                secure=_self.secure,
                connect_timeout=_self.request_timeout_seconds,
            )

            df = upstream
            if _self.column_names:
                df = df[_self.column_names]

            context.log.info(
                f"ClickHouse insert: {_self.database}.{_self.table} "
                f"({len(df)} rows, {len(df.columns)} cols)"
            )
            client.insert_df(table=_self.table, df=df, database=_self.database)

            # Quick row-count check via SELECT count() for the metadata.
            try:
                total_rows = client.query(
                    f"SELECT count() FROM {_self.database}.{_self.table}"
                ).result_rows[0][0]
            except Exception:
                total_rows = None

            metadata: Dict[str, Any] = {
                "row_count": dg.MetadataValue.int(len(df)),
                "column_count": dg.MetadataValue.int(len(df.columns)),
                "clickhouse_table": f"{_self.database}.{_self.table}",
            }
            if total_rows is not None:
                metadata["clickhouse/table_total_rows"] = dg.MetadataValue.int(int(total_rows))

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
