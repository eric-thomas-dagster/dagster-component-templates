"""MotherDuck IO Manager component — reads and writes DataFrames via MotherDuck (serverless DuckDB)."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class MotherDuckIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DuckDB IO manager connected to MotherDuck (serverless DuckDB cloud) for asset storage."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    motherduck_token_env_var: str = Field(default="MOTHERDUCK_TOKEN", description="Environment variable holding the MotherDuck service token")
    database: str = Field(default="my_db", description="MotherDuck database name, e.g. 'my_db'")
    schema_name: Optional[str] = Field(default="main", description="Schema within the MotherDuck database")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb_pandas import DuckDBPandasIOManager

        token = os.environ.get(self.motherduck_token_env_var, "")
        connection_string = f"md:{self.database}?motherduck_token={token}"

        io_manager = DuckDBPandasIOManager(
            database=connection_string,
            schema=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
