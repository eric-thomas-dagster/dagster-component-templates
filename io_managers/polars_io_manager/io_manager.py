"""PolarsIOManager.

ConfigurableIOManager that reads/writes polars.DataFrame as Parquet. Same disk layout as local_parquet_io_manager but with the Polars query engine — much faster on large rows / wide tables.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from pathlib import Path as _P
import polars as pl


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class PolarsIOManager(dg.ConfigurableIOManager):
    """Persist Polars DataFrames as Parquet files locally (alternative to pandas)."""

    base_dir: str = Field(default="dagster_storage", description="Local directory.")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not hasattr(obj, "write_parquet"):
            raise TypeError(f"PolarsIOManager expects a polars.DataFrame; got {type(obj)}")
        path = _P(self.base_dir) / _P(*[_sanitize(p) for p in (list(context.asset_key.path) + ([context.asset_partition_key] if context.has_asset_partitions else []))]).with_suffix(".parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        obj.write_parquet(path)
        context.add_output_metadata({"path": dg.MetadataValue.path(str(path.resolve())), "row_count": dg.MetadataValue.int(obj.height)})

    def load_input(self, context):
        up = context.upstream_output
        path = _P(self.base_dir) / _P(*[_sanitize(p) for p in (list(up.asset_key.path) + ([up.asset_partition_key] if up.has_asset_partitions else []))]).with_suffix(".parquet")
        return pl.read_parquet(path)
