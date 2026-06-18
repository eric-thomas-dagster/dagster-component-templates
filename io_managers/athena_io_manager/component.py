"""Athena IO Manager component.

YAML/Component wrapper around `AthenaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Union

import dagster as dg
from pydantic import Field

from .io_manager import AthenaIOManager


class AthenaIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Athena IO manager that stores assets as Parquet on S3 and queries them via Amazon Athena.

    This manager does **two** things on every materialization:
      1. Writes a Parquet dataset to ``s3_output_location/<database>/<table>/``
      2. Registers/updates the table in the AWS Glue Data Catalog so Athena can query it

    Features:
      - Partition-aware writes: ``mode='overwrite_partitions'`` rewrites only the
        Hive partition matching ``partition_column = '<partition_key>'``
      - Multi-component asset keys map to ``database.table`` (first component is the database)
      - Output metadata records the qualified table name, S3 path, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's files

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    database: str = Field(description="Athena database (Glue catalog database)")
    s3_staging_dir: str = Field(description="S3 URI for Athena query results, e.g. 's3://my-bucket/athena-results/'")
    s3_output_location: Optional[str] = Field(
        default=None,
        description="S3 URI prefix where Parquet assets are written, e.g. 's3://my-bucket/assets/'",
    )
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS access key (uses instance role if not set)",
    )
    aws_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS secret key",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Hive partition column name used when writing partitioned assets",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = AthenaIOManager(
            database=self.database,
            s3_staging_dir=self.s3_staging_dir,
            s3_output_location=self.s3_output_location,
            region_name=self.region_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_env_var) if self.aws_access_key_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_key_env_var) if self.aws_secret_key_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
