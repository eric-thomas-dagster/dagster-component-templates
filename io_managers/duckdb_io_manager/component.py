"""DuckDB IO Manager component — reads and writes Pandas DataFrames via a local DuckDB file."""
from dataclasses import dataclass
from typing import Optional
import dagster as dg
from pydantic import Field


@dataclass
class DuckDBIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DuckDBPandasIOManager so assets are automatically stored in and loaded from a local DuckDB database."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    database: str = Field(default="dagster.duckdb", description="Path to the DuckDB database file, e.g. 'data/warehouse.duckdb'")
    schema_name: Optional[str] = Field(default="main", description="Schema within the DuckDB database for asset tables")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb_pandas import DuckDBPandasIOManager
        io_manager = DuckDBPandasIOManager(
            database=self.database,
            schema=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
