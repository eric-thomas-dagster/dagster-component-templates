"""LocalJsonIOManager.

Stores pandas DataFrames as newline-delimited JSON (`.jsonl`), one record per line. Friendlier than CSV for nested / dict-typed columns — round-trips Python objects through json. Good for asset shapes that include arbitrary metadata.

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


class LocalJsonIOManager(dg.ConfigurableIOManager):
    """Reads/writes pandas DataFrames as jsonl files in a local directory."""

    base_dir: str = Field(default="dagster_storage", description="Local directory under which to store assets.")
    create_dir: bool = Field(default=True, description="Create base_dir if missing.")

    def _path_for(self, context) -> Path:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        sanitized = [_sanitize(str(p)) for p in parts]
        return Path(self.base_dir) / Path(*sanitized).with_suffix(".jsonl")

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        path = self._path_for(context)
        if self.create_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        obj.to_json(path, orient="records", lines=True)
        context.add_output_metadata({
            "path": dg.MetadataValue.path(str(path.resolve())),
            "row_count": dg.MetadataValue.int(len(obj)),
        })

    def load_input(self, context) -> pd.DataFrame:
        path = self._path_for(context.upstream_output)
        return pd.read_json(path, orient="records", lines=True)
