"""MinIO IO Manager component — reads and writes Delta Lake tables via MinIO (S3-compatible)."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class MinIOIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Delta Lake IO manager pointed at a MinIO endpoint for S3-compatible lakehouse storage."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    endpoint_url: str = Field(default="http://localhost:9000", description="MinIO endpoint URL, e.g. 'http://localhost:9000'")
    access_key_env_var: str = Field(default="MINIO_ACCESS_KEY", description="Environment variable holding the MinIO access key")
    secret_key_env_var: str = Field(default="MINIO_SECRET_KEY", description="Environment variable holding the MinIO secret key")
    bucket: str = Field(default="lake", description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(default="assets", description="Key prefix within the bucket for asset tables")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        endpoint_url = self.endpoint_url
        access_key = os.environ.get(self.access_key_env_var, "")
        secret_key = os.environ.get(self.secret_key_env_var, "")
        bucket = self.bucket
        prefix = self.prefix

        @dg.io_manager
        def minio_delta_io_manager(init_context):
            class _MinioDeltaIOManager(dg.IOManager):
                def _get_storage_options(self):
                    return {
                        "AWS_ENDPOINT_URL": endpoint_url,
                        "AWS_ACCESS_KEY_ID": access_key,
                        "AWS_SECRET_ACCESS_KEY": secret_key,
                        "AWS_ALLOW_HTTP": "true",
                        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                    }

                def _table_path(self, context):
                    table = context.asset_key.path[-1]
                    return f"s3://{bucket}/{prefix}/{table}"

                def handle_output(self, context, obj: pd.DataFrame):
                    from deltalake.writer import write_deltalake
                    path = self._table_path(context)
                    write_deltalake(
                        path,
                        obj,
                        mode="overwrite",
                        storage_options=self._get_storage_options(),
                    )
                    context.add_output_metadata({"row_count": len(obj), "delta_path": path})

                def load_input(self, context):
                    from deltalake import DeltaTable
                    path = self._table_path(context)
                    dt = DeltaTable(path, storage_options=self._get_storage_options())
                    return dt.to_pandas()

            return _MinioDeltaIOManager()

        return dg.Definitions(resources={self.resource_key: minio_delta_io_manager})
