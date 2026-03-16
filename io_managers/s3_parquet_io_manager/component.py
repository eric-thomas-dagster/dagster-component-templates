"""S3 Parquet IO Manager component — reads and writes Pandas DataFrames as Parquet files on S3."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class S3ParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Amazon S3."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    bucket: str = Field(description="S3 bucket to store Parquet files in")
    prefix: str = Field(default="assets", description="S3 key prefix for asset files, e.g. 'dagster/assets'")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS access key (uses instance role if not set)")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS secret key")
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3-compatible endpoint URL (e.g. for MinIO: 'http://localhost:9000')")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        bucket = self.bucket
        prefix = self.prefix
        region_name = self.region_name
        access_key = os.environ.get(self.aws_access_key_env_var, "") if self.aws_access_key_env_var else None
        secret_key = os.environ.get(self.aws_secret_key_env_var, "") if self.aws_secret_key_env_var else None
        endpoint_url = self.endpoint_url

        @dg.io_manager
        def s3_parquet_io_manager(init_context):
            class _S3ParquetIOManager(dg.IOManager):
                def _get_fs(self):
                    import s3fs
                    kwargs = {}
                    if access_key and secret_key:
                        kwargs["key"] = access_key
                        kwargs["secret"] = secret_key
                    if region_name:
                        kwargs.setdefault("client_kwargs", {})["region_name"] = region_name
                    if endpoint_url:
                        kwargs.setdefault("client_kwargs", {})["endpoint_url"] = endpoint_url
                    return s3fs.S3FileSystem(**kwargs)

                def _object_path(self, context):
                    table = context.asset_key.path[-1]
                    return f"{bucket}/{prefix}/{table}.parquet"

                def handle_output(self, context, obj: pd.DataFrame):
                    fs = self._get_fs()
                    path = self._object_path(context)
                    with fs.open(path, "wb") as f:
                        obj.to_parquet(f, index=False)
                    context.add_output_metadata({"row_count": len(obj), "s3_path": f"s3://{path}"})

                def load_input(self, context):
                    fs = self._get_fs()
                    path = self._object_path(context)
                    with fs.open(path, "rb") as f:
                        return pd.read_parquet(f)

            return _S3ParquetIOManager()

        return dg.Definitions(resources={self.resource_key: s3_parquet_io_manager})
