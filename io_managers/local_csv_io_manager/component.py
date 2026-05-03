"""LocalCsvIOManagerComponent.

YAML/Component wrapper around `LocalCsvIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field

from .io_manager import LocalCsvIOManager


class LocalCsvIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as CSV files on the local filesystem (dev / human-inspectable)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    base_dir: str = Field(
        default="dagster_storage",
        description="Local directory under which to store assets, e.g. '/tmp/dagster_data'.",
    )
    create_dir: bool = Field(
        default=True,
        description="Create base_dir if it doesn't exist.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LocalCsvIOManager(
            base_dir=self.base_dir,
            create_dir=self.create_dir,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
