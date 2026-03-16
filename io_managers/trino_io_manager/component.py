"""Trino IO Manager component — reads and writes DataFrames via Trino SQL."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class TrinoIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a custom Trino IO manager that reads and writes Pandas DataFrames via Trino SQL."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Trino catalog, e.g. 'iceberg', 'hive', 'delta'")
    schema_name: str = Field(default="default", description="Default schema for asset tables")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Trino password")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        host = self.host
        port = self.port
        user = self.user
        catalog = self.catalog
        schema_name = self.schema_name
        password_env_var = self.password_env_var

        @dg.io_manager
        def trino_pandas_io_manager(init_context):
            class _TrinoIOManager(dg.IOManager):
                def _get_connection(self):
                    import trino.dbapi as trino_dbapi
                    password = os.environ.get(password_env_var, "") if password_env_var else None
                    return trino_dbapi.connect(
                        host=host,
                        port=port,
                        user=user,
                        catalog=catalog,
                        schema=schema_name,
                        auth=trino_dbapi.auth.BasicAuthentication(user, password) if password else None,
                    )

                def handle_output(self, context, obj: pd.DataFrame):
                    table = context.asset_key.path[-1]
                    qualified = f"{catalog}.{schema_name}.{table}"
                    cols = ", ".join(
                        f"{c} {'DOUBLE' if str(obj[c].dtype).startswith('float') else 'BIGINT' if str(obj[c].dtype).startswith('int') else 'VARCHAR'}"
                        for c in obj.columns
                    )
                    conn = self._get_connection()
                    cur = conn.cursor()
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({cols})")
                    cur.execute(f"DELETE FROM {qualified}")
                    values = ", ".join(
                        "(" + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in row) + ")"
                        for _, row in obj.iterrows()
                    )
                    if values:
                        cur.execute(f"INSERT INTO {qualified} VALUES {values}")
                    conn.close()
                    context.add_output_metadata({"row_count": len(obj), "table": qualified})

                def load_input(self, context):
                    table = context.asset_key.path[-1]
                    qualified = f"{catalog}.{schema_name}.{table}"
                    conn = self._get_connection()
                    df = pd.read_sql(f"SELECT * FROM {qualified}", conn)
                    conn.close()
                    return df

            return _TrinoIOManager()

        return dg.Definitions(resources={self.resource_key: trino_pandas_io_manager})
