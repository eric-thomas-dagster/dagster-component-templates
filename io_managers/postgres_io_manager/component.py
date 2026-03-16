"""Postgres IO Manager component — reads and writes Pandas DataFrames as PostgreSQL tables."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class PostgresIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PostgreSQL IO manager so assets are automatically stored in and loaded from Postgres tables."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="PostgreSQL database name")
    user: str = Field(description="PostgreSQL username")
    password_env_var: str = Field(description="Environment variable holding the PostgreSQL password")
    schema_name: Optional[str] = Field(default="public", description="Schema for asset tables")
    if_exists: str = Field(default="replace", description="Behavior when table already exists: 'replace', 'append', or 'fail'")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        host = self.host
        port = self.port
        database = self.database
        user = self.user
        password_env_var = self.password_env_var
        schema_name = self.schema_name or "public"
        if_exists = self.if_exists

        @dg.io_manager
        def postgres_pandas_io_manager(init_context):
            class _PostgresIOManager(dg.IOManager):
                def _get_engine(self):
                    import sqlalchemy as sa
                    password = os.environ.get(password_env_var, "")
                    return sa.create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

                def handle_output(self, context, obj: pd.DataFrame):
                    table = context.asset_key.path[-1]
                    engine = self._get_engine()
                    obj.to_sql(table, engine, schema=schema_name, if_exists=if_exists, index=False)
                    context.add_output_metadata({"row_count": len(obj), "table": f"{schema_name}.{table}"})

                def load_input(self, context):
                    table = context.asset_key.path[-1]
                    engine = self._get_engine()
                    import sqlalchemy as sa
                    with engine.connect() as conn:
                        return pd.read_sql(sa.text(f'SELECT * FROM "{schema_name}"."{table}"'), conn)

            return _PostgresIOManager()

        return dg.Definitions(resources={self.resource_key: postgres_pandas_io_manager})
