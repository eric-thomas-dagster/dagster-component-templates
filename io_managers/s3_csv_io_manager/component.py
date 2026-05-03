"""S3CsvIOManagerComponent.

YAML/Component wrapper around `S3CsvIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import S3CsvIOManager


class S3CsvIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as CSV files on Amazon S3 (or any S3-compatible endpoint)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    bucket: str = Field(description="S3 bucket.")
    prefix: str = Field(default="dagster/assets", description="Key prefix.")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Env var for access key.")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Env var for secret key.")
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = S3CsvIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            region_name=self.region_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_env_var).get_value() if self.aws_access_key_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_key_env_var).get_value() if self.aws_secret_key_env_var else None,
            endpoint_url=self.endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
