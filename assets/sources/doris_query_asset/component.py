"""Apache Doris query asset — read SQL → DataFrame.

Materializes by running a SQL query against Apache Doris via the MySQL
wire protocol and returning the result as a Pandas DataFrame. Use this
for "I want to pull data OUT of Doris into Dagster" — the symmetric
counterpart to ``dataframe_to_doris``.

Doris speaks the MySQL protocol, so the read path is just
SQLAlchemy + PyMySQL — no Doris-specific REST involved. The component
exists for discoverability (so customers searching "doris" find a
ready-made read-component) and to surface Doris-shaped metadata on the
materialized asset.

For multi-step SQL pipelines (CTAS, MERGE, etc.), use the generic
``sql_transform`` component against ``doris_resource.connection_string``
— this asset is for the single-query → single-DataFrame read shape.
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DorisQueryAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a SQL query against Apache Doris and emit the result as a DataFrame asset.

    Example:

        ```yaml
        type: dagster_community_components.DorisQueryAssetComponent
        attributes:
          asset_name: doris_orders_recent
          query: |
            SELECT order_id, customer_id, total, order_date
              FROM analytics.orders
             WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAY
          host_env_var: DORIS_FE_HOST
          query_port: 9030
          database: analytics
          username_env_var: DORIS_USER
          password_env_var: DORIS_PASSWORD
        ```

    Pair with ``doris_resource`` if you have multiple Doris assets — they
    can share one connection via ``connection_string_env_var``. For a
    single asset, the env-var fields below are enough.
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    query: str = Field(description="SQL query to execute. Returns the full result set as a DataFrame.")

    # Connection — either explicit env vars OR a connection_string_env_var.
    host_env_var: Optional[str] = Field(default="DORIS_FE_HOST", description="Env var with Doris FE host.")
    query_port: int = Field(default=9030, description="Doris MySQL-protocol query port.")
    database: Optional[str] = Field(default=None, description="Doris database (catalog).")
    username_env_var: Optional[str] = Field(default="DORIS_USER", description="Env var with username.")
    password_env_var: Optional[str] = Field(default="DORIS_PASSWORD", description="Env var with password.")
    ssl: bool = Field(default=False, description="Connect via TLS.")
    connection_string_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var with a full SQLAlchemy URL "
            "(mysql+pymysql://user:pw@host:9030/db). Overrides the field-by-field "
            "host/port/database/username/password configuration."
        ),
    )

    chunksize: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional pandas read_sql chunksize for streaming-large-result-set "
            "reads. Leave unset for full-materialize-in-memory (typical)."
        ),
    )

    group_name: Optional[str] = Field(default="doris", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'doris').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("doris")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or "Read query result from Apache Doris into a Pandas DataFrame.",
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            try:
                from sqlalchemy import create_engine
            except ImportError:
                raise ImportError("doris_query_asset requires `sqlalchemy>=2.0`: pip install sqlalchemy pymysql")

            if _self.connection_string_env_var and os.environ.get(_self.connection_string_env_var):
                conn_str = os.environ[_self.connection_string_env_var]
            else:
                host = os.environ.get(_self.host_env_var or "", "")
                user = os.environ.get(_self.username_env_var or "", "")
                pw = os.environ.get(_self.password_env_var or "", "")
                if not host or not user:
                    raise RuntimeError(
                        f"Doris connection missing: set {_self.connection_string_env_var or _self.host_env_var} "
                        f"+ {_self.username_env_var} (+ password)."
                    )
                import urllib.parse
                pw_q = urllib.parse.quote_plus(pw)
                conn_str = f"mysql+pymysql://{user}:{pw_q}@{host}:{_self.query_port}/{_self.database or ''}"
                if _self.ssl:
                    conn_str += "?ssl=true"

            engine = create_engine(conn_str)
            context.log.info(f"Executing Doris query against {engine.url.host}:{engine.url.port}")
            df = pd.read_sql(_self.query, engine, chunksize=_self.chunksize)
            if _self.chunksize:
                df = pd.concat(df, ignore_index=True)
            metadata: Dict[str, Any] = {
                "row_count": dg.MetadataValue.int(len(df)),
                "column_count": dg.MetadataValue.int(len(df.columns)),
                "doris_columns": dg.MetadataValue.json({c: str(df[c].dtype) for c in df.columns}),
            }
            if len(df) > 0:
                preview_n = min(25, len(df))
                metadata["preview"] = dg.MetadataValue.md(df.head(preview_n).to_markdown(index=False))
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
