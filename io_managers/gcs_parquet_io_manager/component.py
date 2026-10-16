"""GCS Parquet IO Manager component.

YAML/Component wrapper around `GCSParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid GCS keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


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


class GCSParquetIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on GCS.

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition object. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(default="dagster/assets", description="Object prefix under the bucket")
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default if empty)")
    gcp_credentials_json: Optional[str] = Field(
        default=None,
        description="JSON service-account key contents (resolved from env via dg.EnvVar). Uses ADC if empty.",
    )

    def _get_fs(self):
        import gcsfs
        kwargs: dict[str, Any] = {}
        if self.project:
            kwargs["project"] = self.project
        if self.gcp_credentials_json:
            import json
            kwargs["token"] = json.loads(self.gcp_credentials_json)
        return gcsfs.GCSFileSystem(**kwargs)

    def _object_path(self, context, partition_key=None) -> str:
        """Build the GCS path for an asset (or output).

        Layout: ``<bucket>/<prefix>/<asset_path>/<partition_key>.parquet``
        For non-partitioned assets the partition segment is omitted:
        ``<bucket>/<prefix>/<asset_path>.parquet``
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if partition_key is not None:
            return f"{self.bucket}/{self.prefix}/{base}/{_sanitize(partition_key)}.parquet"
        if context.has_partition_key:
            return f"{self.bucket}/{self.prefix}/{base}/{_sanitize(context.partition_key)}.parquet"
        return f"{self.bucket}/{self.prefix}/{base}.parquet"

    def _write(self, fs, path: str, df: pd.DataFrame) -> None:
        with fs.open(path, "wb") as f:
            df.to_parquet(f, index=False)

    def _read(self, fs, path: str) -> pd.DataFrame:
        with fs.open(path, "rb") as f:
            return pd.read_parquet(f)

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if _is_partition_range(context):
            items = _range_items(context, obj, "GCSParquetIOManager")
            fs = self._get_fs()
            paths = []
            for pkey, df in items:
                path = self._object_path(context, pkey)
                self._write(fs, path, df)
                paths.append(f"gs://{path}")
            context.add_output_metadata(
                {
                    "object_paths": dg.MetadataValue.json(paths),
                    "row_count": dg.MetadataValue.int(sum(len(df) for _, df in items)),
                    "partition_key": dg.MetadataValue.text(_range_label(context)),
                }
            )
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"GCSParquetIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        path = self._object_path(context)
        self._write(self._get_fs(), path, obj)
        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(f"gs://{path}"),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        # Use the upstream's output context to compute the path.
        up = context.upstream_output
        if _is_partition_range(up):
            fs = self._get_fs()
            return {pkey: self._read(fs, self._object_path(up, pkey)) for pkey in up.asset_partition_keys}
        return self._read(self._get_fs(), self._object_path(up))


class GCSParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Google Cloud Storage.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet``
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested GCS paths, preventing collisions between assets of the same name
      - Output metadata records the GCS URI, row count, and partition key
      - Authenticates via ADC by default, or a JSON service-account key

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(
        default="dagster/assets",
        description="GCS object prefix for asset files, e.g. 'dagster/assets'",
    )
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default if empty)")
    gcp_credentials_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding a JSON service-account key. Leave empty to use Application Default Credentials.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = GCSParquetIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            project=self.project,
            gcp_credentials_json=dg.EnvVar(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
