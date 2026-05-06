"""Microsoft Fabric Lakehouse resource.

Resolves the OneLake URL + storage credentials so other ops/assets can
read/write Delta tables in a Fabric Lakehouse without re-deriving the
URL pattern themselves. Mirrors the pattern of postgres_resource /
mssql_resource.

OneLake URL pattern:
  abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/<sub_path>

Common sub_paths:
  Tables/<table>            — Delta tables (auto-registered in SQL endpoint)
  Files/<path>              — raw files (parquet, csv, etc.)
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class FabricLakehouseResource(dg.ConfigurableResource):
    """OneLake URL + storage options helper for Fabric Lakehouse access."""

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def table_path(self, table_name: str) -> str:
        """Return the OneLake abfss URL for a Delta table."""
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_name}.Lakehouse/Tables/{table_name}"
        )

    def file_path(self, sub_path: str) -> str:
        """Return the OneLake abfss URL for a raw file under Files/."""
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_name}.Lakehouse/Files/{sub_path.lstrip('/')}"
        )

    def storage_options(self) -> dict:
        """Return the storage_options dict for delta-rs / pyarrow."""
        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return {
                "azure_tenant_id": tenant,
                "azure_client_id": client,
                "azure_client_secret": secret,
            }
        # Fallback to managed identity / az login
        return {"use_azure_cli": "true"}


class FabricLakehouseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FabricLakehouseResource for use by other components / ops."""

    resource_key: str = Field(default="fabric_lakehouse", description="Dagster resource key")
    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FabricLakehouseResource(
            workspace_id=self.workspace_id,
            lakehouse_name=self.lakehouse_name,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
