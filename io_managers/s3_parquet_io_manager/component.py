"""S3 Parquet IO Manager component.

YAML/Component wrapper around `S3ParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import S3ParquetIOManager


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
