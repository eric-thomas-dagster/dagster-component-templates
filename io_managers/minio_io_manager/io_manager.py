"""MinIO Delta IO Manager.

Stores Dagster assets as Delta Lake tables on MinIO (or any S3-compatible
endpoint). Supports partitioned assets and multi-component asset keys; each
partition is rewritten in place using Delta's overwrite-by-predicate
semantics so backfills don't clobber other partitions.

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid S3 keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


class MinIODeltaIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Delta tables on MinIO."""

    bucket: str = Field(description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(default="dagster/assets", description="Key prefix within the bucket")
    endpoint_url: str = Field(default="http://localhost:9000", description="MinIO endpoint URL")
    access_key: Optional[str] = Field(default=None, description="MinIO access key (resolved from env via dg.EnvVar)")
    secret_key: Optional[str] = Field(default=None, description="MinIO secret key (resolved from env via dg.EnvVar)")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition overwrites in handle_output / load_input",
    )

    def _get_storage_options(self) -> dict[str, str]:
        opts: dict[str, str] = {
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        if self.access_key:
            opts["AWS_ACCESS_KEY_ID"] = self.access_key
        if self.secret_key:
            opts["AWS_SECRET_ACCESS_KEY"] = self.secret_key
        return opts

    def _table_path(self, context) -> str:
        """Build the Delta table path for an asset (or output).

        Layout: ``s3://<bucket>/<prefix>/<asset_path>``
        Partition data goes inside the table directory; per-partition writes
        use Delta's predicate-based overwrite to scope to a single partition.
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        return f"s3://{self.bucket}/{self.prefix}/{base}"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"MinIODeltaIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        from deltalake.writer import write_deltalake

        path = self._table_path(context)
        storage_options = self._get_storage_options()

        if context.has_partition_key:
            partition_value = _sanitize(context.partition_key)
            df = obj.copy()
            df[self.partition_column] = partition_value
            write_deltalake(
                path,
                df,
                mode="overwrite",
                partition_by=[self.partition_column],
                predicate=f"{self.partition_column} = '{partition_value}'",
                storage_options=storage_options,
            )
        else:
            write_deltalake(
                path,
                obj,
                mode="overwrite",
                storage_options=storage_options,
            )

        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(path),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        from deltalake import DeltaTable

        upstream = context.upstream_output
        path = self._table_path(upstream)
        dt = DeltaTable(path, storage_options=self._get_storage_options())
        if upstream.has_partition_key:
            partition_value = _sanitize(upstream.partition_key)
            return dt.to_pandas(partitions=[(self.partition_column, "=", partition_value)])
        return dt.to_pandas()
