"""LocalJsonIOManagerComponent.

YAML/Component wrapper around `LocalJsonIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
import os
from pathlib import Path
from typing import Any
from collections.abc import Mapping
import pandas as pd


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


def _is_partition_range(context) -> bool:
    """True when the context covers a partition RANGE (BackfillPolicy.single_run):
    it has asset partitions but no single partition_key."""
    return bool(context.has_asset_partitions) and not context.has_partition_key


def _range_label(context) -> str:
    keys = list(context.asset_partition_keys)
    return f"{keys[0]}...{keys[-1]}" if len(keys) > 1 else str(keys[0])


def _range_items(context, obj, who: str):
    """Validate a range write: obj must be {partition_key: DataFrame} for exactly the range's keys."""
    keys = list(context.asset_partition_keys)
    if not isinstance(obj, Mapping):
        raise TypeError(
            f"{who}: this output covers a partition range ({_range_label(context)}), e.g. a "
            f"BackfillPolicy.single_run() backfill. A multi-partition output must be a dict "
            f"{{partition_key: DataFrame}} with one entry per partition key {keys}; got {type(obj).__name__}."
        )
    missing = [k for k in keys if k not in obj]
    extra = [k for k in obj if k not in keys]
    if missing or extra:
        raise ValueError(
            f"{who}: dict keys must equal the partition keys of the range. "
            f"Missing: {missing}; unexpected: {extra}."
        )
    for k in keys:
        if not isinstance(obj[k], pd.DataFrame):
            raise TypeError(f"{who}: value for partition {k!r} must be a DataFrame; got {type(obj[k]).__name__}.")
    return [(k, obj[k]) for k in keys]


class LocalJsonIOManager(dg.ConfigurableIOManager):
    """Reads/writes pandas DataFrames as jsonl files in a local directory.

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition file. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    base_dir: str = Field(default="dagster_storage", description="Local directory under which to store assets.")
    create_dir: bool = Field(default=True, description="Create base_dir if missing.")

    def _path_for(self, context, partition_key=None) -> Path:
        parts = list(context.asset_key.path)
        if partition_key is not None:
            parts.append(partition_key)
        elif context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        sanitized = [_sanitize(str(p)) for p in parts]
        return Path(self.base_dir) / Path(*sanitized).with_suffix(".jsonl")

    def _write(self, path: Path, obj: pd.DataFrame) -> None:
        if self.create_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        obj.to_json(path, orient="records", lines=True)

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        if _is_partition_range(context):
            items = _range_items(context, obj, "LocalJsonIOManager")
            paths = []
            for key, df in items:
                path = self._path_for(context, key)
                self._write(path, df)
                paths.append(str(path.resolve()))
            context.add_output_metadata({
                "paths": dg.MetadataValue.json(paths),
                "partition_key": dg.MetadataValue.text(_range_label(context)),
                "row_count": dg.MetadataValue.int(sum(len(df) for _, df in items)),
            })
            return
        path = self._path_for(context)
        self._write(path, obj)
        context.add_output_metadata({
            "path": dg.MetadataValue.path(str(path.resolve())),
            "row_count": dg.MetadataValue.int(len(obj)),
        })

    def load_input(self, context):
        upstream = context.upstream_output
        if _is_partition_range(upstream):
            return {
                key: pd.read_json(self._path_for(upstream, key), orient="records", lines=True)
                for key in upstream.asset_partition_keys
            }
        path = self._path_for(upstream)
        return pd.read_json(path, orient="records", lines=True)


class LocalJsonIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as JSON Lines (.jsonl) files on the local filesystem."""

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
        io_manager = LocalJsonIOManager(
            base_dir=self.base_dir,
            create_dir=self.create_dir,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
