"""Trino Query Asset Component.

Execute Trino SQL and materialize the result as a Dagster asset.
"""

from typing import Dict, List, Optional

import pandas as pd
import dagster as dg
from pydantic import Field


class TrinoQueryComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for executing Trino queries and materializing results.

    Runs a Trino SQL query via the `trino` Python client and returns the
    result as a pandas DataFrame asset.

    Example:
        ```yaml
        type: dagster_component_templates.TrinoQueryComponent
        attributes:
          asset_name: federated_pnl
          host: localhost
          port: 8080
          catalog: postgres
          schema_name: finance
          query: |
            SELECT account, SUM(debit - credit) AS balance
            FROM finance.gl_entries
            GROUP BY account
          group_name: finance
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(description="Trino catalog, e.g. 'postgres', 'iceberg', 'hive'")
    schema_name: Optional[str] = Field(
        default=None, description="Default schema within the catalog"
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Trino password (basic auth). Omit for no auth.",
    )
    query: str = Field(description="Trino SQL query to execute")

    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys (e.g. ['raw_orders', 'sales/dim_customer'])",
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None, description="Additional key-value tags"
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['trino'] if not set.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        host = self.host
        port = self.port
        user = self.user
        catalog = self.catalog
        schema_name = self.schema_name
        password_env_var = self.password_env_var
        query = self.query
        group_name = self.group_name
        description = self.description or f"Trino query: {query[:60].strip()}..."

        kinds = self.kinds or ["trino"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=description,
            group_name=group_name,
            owners=self.owners or [],
            tags=tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def trino_query_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            import trino.dbapi as trino_dbapi
            from trino.auth import BasicAuthentication

            import os

            password = os.environ.get(password_env_var) if password_env_var else None
            context.log.info(f"Connecting to Trino at {host}:{port} (catalog={catalog})")
            conn = trino_dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema_name,
                auth=BasicAuthentication(user, password) if password else None,
            )
            try:
                context.log.info(f"Executing query: {query[:100]}")
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                context.log.info(f"Query returned {len(df)} rows, {len(cols)} columns")

                schema = dg.TableSchema(
                    columns=[
                        dg.TableColumn(name=str(c), type=str(df.dtypes[c]))
                        for c in df.columns
                    ]
                )
                context.add_output_metadata(
                    {
                        "dagster/row_count": dg.MetadataValue.int(len(df)),
                        "dagster/column_schema": dg.MetadataValue.table_schema(schema),
                    }
                )
                return df
            finally:
                conn.close()

        return dg.Definitions(assets=[trino_query_asset])
