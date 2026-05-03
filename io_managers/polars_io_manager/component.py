"""PolarsIOManagerComponent.

ConfigurableIOManager that wraps the official `dagster-polars` package's PolarsParquetIOManager. Reads/writes polars.DataFrame as Parquet. Same disk layout as a local-Parquet IO manager but with Polars' query engine — much faster on large rows / wide tables than pandas.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from dagster_polars import PolarsParquetIOManager


class PolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap the official `dagster-polars` PolarsParquetIOManager so polars.DataFrame assets persist as Parquet."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key. Use 'io_manager' to make this the default.")
    base_dir: str = Field(default="dagster_storage", description="Local directory under which to store Parquet files.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = PolarsParquetIOManager(
            base_dir=self.base_dir,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
