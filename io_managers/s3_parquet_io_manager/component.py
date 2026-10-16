"""S3 Parquet IO Manager component.

YAML/Component wrapper around `S3ParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid S3 keys."""
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


class S3ParquetIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on S3.

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition object. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    bucket: str = Field(description="S3 bucket to store Parquet files in")
    prefix: str = Field(default="dagster/assets", description="Key prefix under the bucket")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key (resolved from env via dg.EnvVar)")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret key (resolved from env via dg.EnvVar)")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL for S3-compatible backends (MinIO, LocalStack)")

    def _get_fs(self):
        import s3fs
        kwargs: dict[str, Any] = {}
        if self.aws_access_key_id and self.aws_secret_access_key:
            kwargs["key"] = self.aws_access_key_id
            kwargs["secret"] = self.aws_secret_access_key
        client_kwargs: dict[str, Any] = {}
        if self.region_name:
            client_kwargs["region_name"] = self.region_name
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        if client_kwargs:
            kwargs["client_kwargs"] = client_kwargs
        return s3fs.S3FileSystem(**kwargs)

    def _object_path(self, context, partition_key=None) -> str:
        """Build the S3 path for an asset (or output).

        Layout: ``<bucket>/<prefix>/<asset_path>/<partition_key>.parquet``
        For non-partitioned assets the partition segment is omitted:
        ``<bucket>/<prefix>/<asset_path>.parquet``
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            # Op output (non-asset materialization)
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
            items = _range_items(context, obj, "S3ParquetIOManager")
            fs = self._get_fs()
            paths = []
            for pkey, df in items:
                path = self._object_path(context, pkey)
                self._write(fs, path, df)
                paths.append(f"s3://{path}")
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
                f"S3ParquetIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        path = self._object_path(context)
        self._write(self._get_fs(), path, obj)
        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(f"s3://{path}"),
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


class S3ParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Amazon S3.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet``
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested S3 paths, preventing collisions between assets of the same name
      - Output metadata records the S3 URI, row count, and partition key
      - Works with S3-compatible backends (MinIO, LocalStack) via ``endpoint_url``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(description="S3 bucket to store Parquet files in")
    prefix: str = Field(
        default="dagster/assets",
        description="S3 key prefix for asset files, e.g. 'dagster/assets'",
    )
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for the AWS access key. Leave empty to use boto3's default credential chain (IAM role, ~/.aws, etc.).",
    )
    aws_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for the AWS secret key.",
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3-compatible endpoint URL (e.g. for MinIO: 'http://localhost:9000')",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = S3ParquetIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            region_name=self.region_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_env_var) if self.aws_access_key_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_key_env_var) if self.aws_secret_key_env_var else None,
            endpoint_url=self.endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
