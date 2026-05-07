"""Azure Table Storage → DataFrame.

Read entities from an Azure Table and materialize them as a DataFrame.
Supports OData filter expressions for partition-scoped, RowKey-scoped,
or arbitrary property filtering.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
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


class AzureTableReaderComponent(Component, Model, Resolvable):
    """Read entities from an Azure Table as a DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")

    account_name_env_var: str = Field(default="AZURE_STORAGE_ACCOUNT")
    account_key_env_var: Optional[str] = Field(default="AZURE_STORAGE_ACCOUNT_KEY")
    table_name: str = Field(description="Table to read")

    filter_query: Optional[str] = Field(
        default=None,
        description=(
            "OData filter expression. Examples: "
            "\"PartitionKey eq 'orders' and RowKey gt '2026'\", "
            "\"category eq 'Electronics'\". Leave empty to read all entities."
        ),
    )
    select: Optional[List[str]] = Field(
        default=None,
        description="Optional column projection — list of property names to return",
    )
    limit: Optional[int] = Field(
        default=None,
        description="Optional row cap (client-side; the server returns pages of 1000)",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["azure", "table_storage"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Read entities from Azure Table '{self.table_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def reader(context: AssetExecutionContext) -> MaterializeResult:
            try:
                from azure.data.tables import TableServiceClient
                from azure.identity import DefaultAzureCredential
            except ImportError as e:
                raise ImportError(
                    "azure-data-tables + azure-identity required: "
                    "pip install azure-data-tables azure-identity"
                ) from e

            account = os.environ[cfg.account_name_env_var]
            account_key = (
                os.environ.get(cfg.account_key_env_var) if cfg.account_key_env_var else None
            )
            endpoint = f"https://{account}.table.core.windows.net"

            if account_key:
                from azure.core.credentials import AzureNamedKeyCredential
                cred = AzureNamedKeyCredential(account, account_key)
            else:
                cred = DefaultAzureCredential()

            service = TableServiceClient(endpoint=endpoint, credential=cred)
            client = service.get_table_client(cfg.table_name)

            kwargs = {}
            if cfg.select:
                kwargs["select"] = cfg.select

            if cfg.filter_query:
                entities_iter = client.query_entities(query_filter=cfg.filter_query, **kwargs)
            else:
                entities_iter = client.list_entities(**kwargs)

            rows = []
            for ent in entities_iter:
                rows.append(dict(ent))
                if cfg.limit and len(rows) >= cfg.limit:
                    break

            df = pd.DataFrame(rows)
            context.log.info(f"Read {len(df)} entities from {cfg.table_name}")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "table": MetadataValue.text(f"{account}/{cfg.table_name}"),
                    "filter": MetadataValue.text(cfg.filter_query or "(none)"),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[reader])
