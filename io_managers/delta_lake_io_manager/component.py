"""Delta Lake IO Manager component — reads and writes Pandas DataFrames as Delta tables."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class DeltaLakeIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DeltaLakePandasIOManager so assets are stored as Delta tables on local disk, S3, GCS, or Azure ADLS."""

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
        from dagster_deltalake_pandas import DeltaLakePandasIOManager
        from dagster_deltalake import StorageOptions

        storage_options = {}
        if self.aws_access_key_env_var:
            storage_options["AWS_ACCESS_KEY_ID"] = os.environ.get(self.aws_access_key_env_var, "")
        if self.aws_secret_key_env_var:
            storage_options["AWS_SECRET_ACCESS_KEY"] = os.environ.get(self.aws_secret_key_env_var, "")
        if self.aws_region:
            storage_options["AWS_REGION"] = self.aws_region
        if self.azure_storage_account_env_var:
            storage_options["AZURE_STORAGE_ACCOUNT_NAME"] = os.environ.get(self.azure_storage_account_env_var, "")
        if self.azure_storage_key_env_var:
            storage_options["AZURE_STORAGE_ACCOUNT_KEY"] = os.environ.get(self.azure_storage_key_env_var, "")
        if self.gcp_credentials_env_var:
            storage_options["GOOGLE_SERVICE_ACCOUNT"] = os.environ.get(self.gcp_credentials_env_var, "")

        io_manager = DeltaLakePandasIOManager(
            root_uri=self.root_uri,
            storage_options=StorageOptions(**storage_options) if storage_options else StorageOptions(),
            schema=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
