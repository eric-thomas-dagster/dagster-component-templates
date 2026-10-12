from typing import Union
"""MinIO IO Manager component.

YAML/Component wrapper around `MinIODeltaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
from typing import Optional
import pandas as pd


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



class MinIOIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Delta Lake IO manager pointed at a MinIO endpoint for S3-compatible lakehouse storage.

    Features:
      - Partitioned assets are written with Delta predicate-based overwrite
        so per-partition rewrites don't clobber sibling partitions
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested table paths, preventing collisions between assets of the same name
      - Output metadata records the Delta table URI, row count, and partition key
      - S3-compatible (MinIO, LocalStack, Cloudflare R2, etc.) via ``endpoint_url``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(default="lake", description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(
        default="dagster/assets",
        description="Key prefix within the bucket for asset tables",
    )
    endpoint_url: str = Field(
        default="http://localhost:9000",
        description="MinIO endpoint URL, e.g. 'http://localhost:9000'",
    )
    access_key_env_var: str = Field(
        default="MINIO_ACCESS_KEY",
        description="Environment variable holding the MinIO access key",
    )
    secret_key_env_var: str = Field(
        default="MINIO_SECRET_KEY",
        description="Environment variable holding the MinIO secret key",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition Delta overwrites",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MinIODeltaIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            endpoint_url=self.endpoint_url,
            access_key=dg.EnvVar(self.access_key_env_var) if self.access_key_env_var else None,
            secret_key=dg.EnvVar(self.secret_key_env_var) if self.secret_key_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
