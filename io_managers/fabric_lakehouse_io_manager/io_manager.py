"""Microsoft Fabric Lakehouse IO Manager.

Auto-serializes asset DataFrames as Delta tables in a Fabric Lakehouse.
Asset key becomes the table name (sanitized); each materialization
overwrites or appends per `if_exists`.

Tables auto-register in the Fabric SQL endpoint, so they're queryable
from Fabric Data Warehouse / Power BI / Synapse Serverless without
additional setup.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s).strip("_").lower() or "t"


class FabricLakehouseIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that writes pandas DataFrames as Delta tables in OneLake."""

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    if_exists: str = Field(default="overwrite", description="'overwrite' | 'append'")
    table_prefix: str = Field(default="", description="Optional prefix prepended to all table names")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def _table_name(self, context) -> str:
        parts = [_sanitize(p) for p in context.asset_key.path]
        if context.has_asset_partitions:
            parts.append(_sanitize(str(context.asset_partition_key)))
        name = "__".join(parts)
        return f"{self.table_prefix}{name}" if self.table_prefix else name

    def _table_path(self, context) -> str:
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_name}.Lakehouse/Tables/{self._table_name(context)}"
        )

    def _storage_options(self) -> dict:
        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return {
                "azure_tenant_id": tenant,
                "azure_client_id": client,
                "azure_client_secret": secret,
            }
        return {"use_azure_cli": "true"}

    def handle_output(self, context, obj) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"FabricLakehouseIOManager only handles pandas DataFrames; got {type(obj).__name__}."
            )
        from deltalake import write_deltalake
        path = self._table_path(context)
        write_deltalake(
            path,
            obj,
            mode=self.if_exists,
            storage_options=self._storage_options(),
        )
        context.add_output_metadata({
            "onelake_path": dg.MetadataValue.text(path),
            "table": dg.MetadataValue.text(self._table_name(context)),
            "row_count": dg.MetadataValue.int(len(obj)),
            "if_exists": dg.MetadataValue.text(self.if_exists),
        })

    def load_input(self, context) -> pd.DataFrame:
        from deltalake import DeltaTable
        upstream = context.upstream_output
        path = self._table_path(upstream)
        return DeltaTable(path, storage_options=self._storage_options()).to_pandas()
