"""Databricks IO Manager component — reads and writes DataFrames as Unity Catalog Delta tables via Databricks SQL."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class DatabricksIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks IO manager that reads and writes Pandas DataFrames as Unity Catalog Delta tables via Databricks SQL Connector."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    server_hostname: str = Field(description="Databricks workspace hostname, e.g. 'adb-1234567890.12.azuredatabricks.net'")
    http_path: str = Field(description="HTTP path to the SQL warehouse or cluster, e.g. '/sql/1.0/warehouses/abc123'")
    access_token_env_var: str = Field(description="Environment variable holding the Databricks personal access token")
    catalog: str = Field(default="main", description="Unity Catalog catalog name")
    schema_name: str = Field(default="default", description="Unity Catalog schema (database) for asset tables")
    staging_location: Optional[str] = Field(default=None, description="Cloud storage URI for staging data, e.g. 's3://bucket/staging' or 'abfss://container@account.dfs.core.windows.net/staging'")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        server_hostname = self.server_hostname
        http_path = self.http_path
        access_token_env_var = self.access_token_env_var
        catalog = self.catalog
        schema_name = self.schema_name
        staging_location = self.staging_location

        @dg.io_manager
        def databricks_unity_io_manager(init_context):
            class _DatabricksIOManager(dg.IOManager):
                def _get_connection(self):
                    from databricks import sql as databricks_sql
                    token = os.environ.get(access_token_env_var, "")
                    return databricks_sql.connect(
                        server_hostname=server_hostname,
                        http_path=http_path,
                        access_token=token,
                    )

                def handle_output(self, context, obj: pd.DataFrame):
                    table = context.asset_key.path[-1]
                    qualified = f"`{catalog}`.`{schema_name}`.`{table}`"

                    if staging_location:
                        # Write via staging: upload parquet then COPY INTO
                        import uuid
                        stage_path = f"{staging_location.rstrip('/')}/{table}_{uuid.uuid4().hex}.parquet"
                        obj.to_parquet(stage_path, index=False)
                        conn = self._get_connection()
                        cur = conn.cursor()
                        cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} USING DELTA AS SELECT * FROM parquet.`{stage_path}` WHERE 1=0")
                        cur.execute(f"COPY INTO {qualified} FROM '{stage_path}' FILEFORMAT = PARQUET FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('mergeSchema' = 'true')")
                        conn.close()
                    else:
                        # Write row-by-row via INSERT (small datasets only)
                        conn = self._get_connection()
                        cur = conn.cursor()
                        cols = ", ".join(f"`{c}`" for c in obj.columns)
                        col_types = ", ".join(
                            f"`{c}` {'DOUBLE' if str(obj[c].dtype).startswith('float') else 'BIGINT' if str(obj[c].dtype).startswith('int') else 'STRING'}"
                            for c in obj.columns
                        )
                        cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_types}) USING DELTA")
                        cur.execute(f"DELETE FROM {qualified}")
                        for _, row in obj.iterrows():
                            vals = ", ".join(f"'{v}'" if isinstance(v, str) else ("NULL" if v != v else str(v)) for v in row)
                            cur.execute(f"INSERT INTO {qualified} ({cols}) VALUES ({vals})")
                        conn.close()

                    context.add_output_metadata({"row_count": len(obj), "table": qualified})

                def load_input(self, context):
                    table = context.asset_key.path[-1]
                    qualified = f"`{catalog}`.`{schema_name}`.`{table}`"
                    conn = self._get_connection()
                    cur = conn.cursor()
                    cur.execute(f"SELECT * FROM {qualified}")
                    rows = cur.fetchall()
                    columns = [d[0] for d in cur.description]
                    conn.close()
                    return pd.DataFrame(rows, columns=columns)

            return _DatabricksIOManager()

        return dg.Definitions(resources={self.resource_key: databricks_unity_io_manager})
