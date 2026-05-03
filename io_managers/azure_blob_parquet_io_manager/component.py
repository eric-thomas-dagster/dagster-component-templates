"""AzureBlobParquetIOManagerComponent.

YAML/Component wrapper around `AzureBlobParquetIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import AzureBlobParquetIOManager


class AzureBlobParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as Parquet on Azure Blob Storage."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    account_name: str = Field(description="Azure storage account name.")
    container: str = Field(description="Blob container.")
    prefix: str = Field(default="dagster/assets", description="Blob key prefix.")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with the connection string (else uses DefaultAzureCredential).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = AzureBlobParquetIOManager(
            account_name=self.account_name,
            container=self.container,
            prefix=self.prefix,
            connection_string=dg.EnvVar(self.connection_string_env_var).get_value() if self.connection_string_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
