"""DuckDBPolarsIOManagerComponent.

Polars variant of duckdb_io_manager. Same backend (DuckDB) but Polars instead of pandas. Wraps the official `dagster-duckdb-polars` package.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DuckDBPolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-duckdb-polars's DuckDBPolarsIOManager so polars.DataFrame assets persist to DuckDB."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    database: str = Field(default="dagster.duckdb", description="DuckDB file path.")
    schema_name: Optional[str] = Field(default="main", description="Schema.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb_polars import DuckDBPolarsIOManager
        io_manager = DuckDBPolarsIOManager(database=self.database, schema=self.schema_name)
        return dg.Definitions(resources={self.resource_key: io_manager})

