"""LocalParquetIOManager.

A no-frills IO manager that writes pandas DataFrames as Parquet files in a local directory. Each asset gets its own file; partitioned assets get one file per partition. No database, no cloud — perfect for dev iteration and notebook-style work where you want persistence between runs but not the overhead of a warehouse.

Implemented as a `ConfigurableIOManager` subclass — importable directly
without the Component wrapper if you prefer Python-only configuration.
"""
import os
from pathlib import Path
from typing import Any

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class LocalParquetIOManager(dg.ConfigurableIOManager):
    """Reads/writes pandas DataFrames as parquet files in a local directory."""

    base_dir: str = Field(default="dagster_storage", description="Local directory under which to store assets.")
    create_dir: bool = Field(default=True, description="Create base_dir if missing.")

    def _path_for(self, context) -> Path:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        sanitized = [_sanitize(str(p)) for p in parts]
        return Path(self.base_dir) / Path(*sanitized).with_suffix(".parquet")

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        path = self._path_for(context)
        if self.create_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        obj.to_parquet(path, index=False)
        context.add_output_metadata({
            "path": dg.MetadataValue.path(str(path.resolve())),
            "row_count": dg.MetadataValue.int(len(obj)),
        })

    def load_input(self, context) -> pd.DataFrame:
        path = self._path_for(context.upstream_output)
        return pd.read_parquet(path)
