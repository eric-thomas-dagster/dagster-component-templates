"""LocalCsvIOManagerComponent.

YAML/Component wrapper around `LocalCsvIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
import os
from pathlib import Path
from typing import Any
import pandas as pd


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class LocalCsvIOManager(dg.ConfigurableIOManager):
    """Reads/writes pandas DataFrames as csv files in a local directory."""

    base_dir: str = Field(default="dagster_storage", description="Local directory under which to store assets.")
    create_dir: bool = Field(default=True, description="Create base_dir if missing.")

    def _path_for(self, context) -> Path:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        sanitized = [_sanitize(str(p)) for p in parts]
        return Path(self.base_dir) / Path(*sanitized).with_suffix(".csv")

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        path = self._path_for(context)
        if self.create_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        obj.to_csv(path, index=False)
        context.add_output_metadata({
            "path": dg.MetadataValue.path(str(path.resolve())),
            "row_count": dg.MetadataValue.int(len(obj)),
        })

    def load_input(self, context) -> pd.DataFrame:
        path = self._path_for(context.upstream_output)
        return pd.read_csv(path)



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
