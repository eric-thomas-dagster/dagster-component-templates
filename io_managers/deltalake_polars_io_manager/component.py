"""DeltaLakePolarsIOManagerComponent.

Polars variant of delta_lake_io_manager. Wraps the official `dagster-deltalake-polars` package — Polars DataFrames written/read as Delta Lake tables.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DeltaLakePolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-deltalake-polars so polars.DataFrame assets persist as Delta tables."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    root_uri: str = Field(description="Root URI for Delta tables.")
    schema_name: Optional[str] = Field(default=None, description="Default schema (subdirectory).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_deltalake_polars import DeltaLakePolarsIOManager
        io_manager = DeltaLakePolarsIOManager(
            root_uri=self.root_uri,
            schema=self.schema_name or "",
        )
        return dg.Definitions(resources={self.resource_key: io_manager})

