"""S3 Parquet IO Manager.

Stores Dagster assets as Parquet files on Amazon S3 (or any S3-compatible
endpoint). Supports partitioned assets, multi-component asset keys, and
S3-compatible backends like MinIO via `endpoint_url`.

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


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
