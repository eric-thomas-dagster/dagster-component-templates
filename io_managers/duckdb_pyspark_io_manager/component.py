"""DuckDBPySparkIOManagerComponent.

PySpark variant of duckdb_io_manager. Wraps the official `dagster-duckdb-pyspark` package — DataFrames flow as Spark DataFrames between assets, persisted to local DuckDB between runs.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DuckDBPySparkIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-duckdb-pyspark so PySpark DataFrames persist to DuckDB."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    database: str = Field(default="dagster.duckdb", description="DuckDB file path.")
    schema_name: Optional[str] = Field(default="main", description="Schema.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb_pyspark import DuckDBPySparkIOManager
        io_manager = DuckDBPySparkIOManager(database=self.database, schema=self.schema_name)
        return dg.Definitions(resources={self.resource_key: io_manager})

