"""S3 Parquet IO Manager component.

YAML/Component wrapper around `S3ParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid S3 keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


class S3ParquetIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on S3."""

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

    def _object_path(self, context) -> str:
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
        if context.has_partition_key:
            return f"{self.bucket}/{self.prefix}/{base}/{_sanitize(context.partition_key)}.parquet"
        return f"{self.bucket}/{self.prefix}/{base}.parquet"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            # Asset that returns None (e.g. side-effect-only) — nothing to write.
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"S3ParquetIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        path = self._object_path(context)
        with self._get_fs().open(path, "wb") as f:
            obj.to_parquet(f, index=False)
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
        path = self._object_path(context.upstream_output)
        with self._get_fs().open(path, "rb") as f:
            return pd.read_parquet(f)



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
