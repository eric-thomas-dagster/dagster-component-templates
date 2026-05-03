"""S3CsvIOManager.

ConfigurableIOManager that writes pandas DataFrames as CSV to S3. Slower / lossier than the Parquet variant but readable from any tool. Supports partitioned assets and S3-compatible endpoints (MinIO, LocalStack).
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

import boto3
from io import StringIO


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class S3CsvIOManager(dg.ConfigurableIOManager):
    """Store DataFrames as CSV files on Amazon S3 (or any S3-compatible endpoint)."""

    bucket: str = Field(description="S3 bucket.")
    prefix: str = Field(default="dagster/assets", description="Key prefix.")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    aws_access_key_id: Optional[str] = Field(default=None, description="Access key (resolved from env via dg.EnvVar).")
    aws_secret_access_key: Optional[str] = Field(default=None, description="Secret key.")
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint (MinIO, LocalStack).")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"S3CsvIOManager only handles DataFrames; got {type(obj)}")
        s3 = boto3.client("s3", region_name=self.region_name, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, endpoint_url=self.endpoint_url)
        parts = list(context.asset_key.path) + ([context.asset_partition_key] if context.has_asset_partitions else [])
        key = f"{self.prefix}/" + "/".join(_sanitize(p) for p in parts) + ".csv"
        buf = StringIO()
        obj.to_csv(buf, index=False)
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue().encode())
        context.add_output_metadata({"s3_uri": dg.MetadataValue.text(f"s3://{self.bucket}/{key}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        s3 = boto3.client("s3", region_name=self.region_name, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, endpoint_url=self.endpoint_url)
        up = context.upstream_output
        parts = list(up.asset_key.path) + ([up.asset_partition_key] if up.has_asset_partitions else [])
        key = f"{self.prefix}/" + "/".join(_sanitize(p) for p in parts) + ".csv"
        body = s3.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()
        return pd.read_csv(StringIO(body))
