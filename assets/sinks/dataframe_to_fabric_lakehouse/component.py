"""DataFrame → Microsoft Fabric Lakehouse (OneLake Delta tables).

Writes a DataFrame to a Fabric Lakehouse table as Delta. OneLake is ADLS
Gen2 underneath but uses the abfss://onelake.dfs.fabric.microsoft.com URL
shape — this component handles that URL construction so the user just
provides workspace_id and lakehouse name.

Fabric Lakehouse tables auto-register in the SQL endpoint, so once
written they're queryable from Fabric Data Warehouse + Synapse Serverless
+ Power BI without additional setup.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeToFabricLakehouseComponent(Component, Model, Resolvable):
    """Write a DataFrame to a Microsoft Fabric Lakehouse table (Delta on OneLake)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse name (the .Lakehouse item in the workspace)")
    table_name: str = Field(description="Target Delta table name (created if missing)")
    write_mode: str = Field(
        default="overwrite",
        description="'overwrite' | 'append'. (Delta MERGE not yet supported here.)",
    )

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream = self.upstream_asset_key
        ws = self.workspace_id
        lh = self.lakehouse_name
        table = self.table_name
        write_mode = self.write_mode
        tenant_env = self.tenant_id_env_var
        client_env = self.client_id_env_var
        secret_env = self.client_secret_env_var

        kinds = ["azure", "fabric", "delta"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream))},
            group_name=self.group_name,
            description=self.description or f"Write DataFrame to Fabric Lakehouse '{lh}' table '{table}' (Delta).",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def fabric_lakehouse_writer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.identity import DefaultAzureCredential, ClientSecretCredential
                from deltalake import write_deltalake
            except ImportError as e:
                raise ImportError(
                    "deltalake + azure-identity required: pip install deltalake azure-identity"
                ) from e

            tenant = os.environ.get(tenant_env) if tenant_env else None
            client = os.environ.get(client_env) if client_env else None
            secret = os.environ.get(secret_env) if secret_env else None

            # OneLake URL: abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table>
            onelake_path = (
                f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Tables/{table}"
            )

            # deltalake supports azure_storage_options or storage_options for Azure
            storage_options = {"use_azure_cli": "true"}
            if tenant and client and secret:
                storage_options = {
                    "azure_tenant_id": tenant,
                    "azure_client_id": client,
                    "azure_client_secret": secret,
                }

            context.log.info(
                f"Writing {len(upstream)} rows to Fabric Lakehouse '{lh}' "
                f"table '{table}' (mode={write_mode})"
            )
            write_deltalake(
                onelake_path,
                upstream,
                mode=write_mode,
                storage_options=storage_options,
            )
            context.log.info(f"Wrote to {onelake_path}")

            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "onelake_path": MetadataValue.text(onelake_path),
                    "lakehouse": MetadataValue.text(lh),
                    "table": MetadataValue.text(table),
                    "write_mode": MetadataValue.text(write_mode),
                }
            )

        return Definitions(assets=[fabric_lakehouse_writer])
