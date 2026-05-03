"""PolarsIOManagerComponent.

YAML/Component wrapper around `PolarsIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import PolarsIOManager


class PolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist Polars DataFrames as Parquet files locally (alternative to pandas)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    base_dir: str = Field(default="dagster_storage", description="Local directory.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = PolarsIOManager(
            base_dir=self.base_dir,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
