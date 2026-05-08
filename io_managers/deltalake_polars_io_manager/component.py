"""DeltaLakePolarsIOManagerComponent.

Polars variant of delta_lake_io_manager. Wraps the official `dagster-deltalake-polars` package — Polars DataFrames written/read as Delta Lake tables.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class DeltaLakePolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DeltaLakePolarsIOManager so polars.DataFrame assets persist as Delta tables on local disk, S3, GCS, or Azure ADLS."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    root_uri: str = Field(description="Root path for Delta tables, e.g. '/data/delta', 's3://bucket/delta', 'az://container/delta'")
    schema_name: Optional[str] = Field(default=None, description="Default schema (subdirectory) under root_uri")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS access key (S3 storage)")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS secret key (S3 storage)")
    aws_region: Optional[str] = Field(default=None, description="AWS region for S3 storage, e.g. 'us-east-1'")
    azure_storage_account_env_var: Optional[str] = Field(default=None, description="Environment variable for Azure storage account name (ADLS storage)")
    azure_storage_key_env_var: Optional[str] = Field(default=None, description="Environment variable for Azure storage account key (ADLS storage)")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Environment variable holding a GCS service account JSON key")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_deltalake_polars import DeltaLakePolarsIOManager
        from dagster_deltalake import LocalConfig, S3Config, AzureConfig, GcsConfig

        # Pick a storage_options config based on the URI scheme / credential bundle.
        storage_options = LocalConfig()
        if self.aws_access_key_env_var or self.aws_region or self.root_uri.startswith("s3://"):
            storage_options = S3Config(
                access_key_id=(os.environ.get(self.aws_access_key_env_var, "") if self.aws_access_key_env_var else None),
                secret_access_key=(os.environ.get(self.aws_secret_key_env_var, "") if self.aws_secret_key_env_var else None),
                region=self.aws_region,
            )
        elif self.azure_storage_account_env_var or self.root_uri.startswith(("az://", "abfss://", "abfs://")):
            storage_options = AzureConfig(
                account_name=os.environ.get(self.azure_storage_account_env_var, "") if self.azure_storage_account_env_var else None,
                account_key=os.environ.get(self.azure_storage_key_env_var, "") if self.azure_storage_key_env_var else None,
            )
        elif self.gcp_credentials_env_var or self.root_uri.startswith("gs://"):
            storage_options = GcsConfig(
                service_account=os.environ.get(self.gcp_credentials_env_var, "") if self.gcp_credentials_env_var else None,
            )

        io_manager = DeltaLakePolarsIOManager(
            root_uri=self.root_uri,
            storage_options=storage_options,
            schema=self.schema_name or "",
        )
        return dg.Definitions(resources={self.resource_key: io_manager})

