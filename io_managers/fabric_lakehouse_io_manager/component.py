"""FabricLakehouseIOManagerComponent.

Wires a `FabricLakehouseIOManager` into Dagster as a configurable IO
manager — assets that don't override `io_manager_key` will land as Delta
tables in the configured Fabric Lakehouse on OneLake.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import FabricLakehouseIOManager


class FabricLakehouseIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FabricLakehouseIOManager as a Dagster IO manager."""

    resource_key: str = Field(
        default="io_manager",
        description="IO manager key. 'io_manager' makes this the workspace default.",
    )
    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    if_exists: str = Field(default="overwrite", description="'overwrite' | 'append'")
    table_prefix: str = Field(default="", description="Optional prefix prepended to all table names")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = FabricLakehouseIOManager(
            workspace_id=self.workspace_id,
            lakehouse_name=self.lakehouse_name,
            if_exists=self.if_exists,
            table_prefix=self.table_prefix,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
