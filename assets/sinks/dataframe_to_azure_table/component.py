"""DataFrame to Azure Table Storage.

Writes each DataFrame row as an entity to an Azure Table Storage table.
Azure Tables is a cheap NoSQL key-value store ($0.045/GB/mo + $0.00036 per
10K transactions). Way cheaper than Cosmos DB for simple structured
storage; less powerful (no global distribution, no auto-indexing).

Each entity must have:
- PartitionKey (groups entities for query locality + transactions)
- RowKey (unique within partition)

The component derives these from configurable columns, with sensible
defaults for tables that already match Azure Table conventions.
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


class DataframeToAzureTableComponent(Component, Model, Resolvable):
    """Write each DataFrame row to Azure Table Storage as an entity."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    account_name_env_var: str = Field(
        default="AZURE_STORAGE_ACCOUNT",
        description="Env var holding the storage account name",
    )
    account_key_env_var: Optional[str] = Field(
        default="AZURE_STORAGE_ACCOUNT_KEY",
        description="Env var holding the account key. Omit to use DefaultAzureCredential (Entra OAuth).",
    )
    table_name: str = Field(description="Table name (created if it doesn't exist)")

    partition_key_column: str = Field(
        description="DataFrame column that maps to PartitionKey on each entity",
    )
    row_key_column: str = Field(
        description="DataFrame column that maps to RowKey on each entity (unique within partition)",
    )
    create_if_missing: bool = Field(
        default=True,
        description="Create the table if it doesn't exist (free metadata operation)",
    )
    write_mode: str = Field(
        default="upsert",
        description="'upsert' (replace existing entity), 'merge' (patch existing), or 'insert' (fail on dup)",
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description=(
            "Max entities per batch. Azure caps at 100 entities per transaction; "
            "all entities in a batch must share the same PartitionKey."
        ),
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Write DataFrame rows to Azure Table '{self.table_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def producer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.data.tables import TableServiceClient, UpdateMode, TransactionOperation
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

            if cfg.create_if_missing:
                try:
                    service.create_table(cfg.table_name)
                    context.log.info(f"Created table '{cfg.table_name}'")
                except Exception:
                    pass  # exists already

            client = service.get_table_client(cfg.table_name)

            mode_map = {"upsert": UpdateMode.REPLACE, "merge": UpdateMode.MERGE}

            records = upstream.to_dict(orient="records")
            sent = 0
            errors = []

            # Group by partition key (batches must be intra-partition)
            from itertools import groupby
            from operator import itemgetter

            def _entity(r):
                return {
                    **{k: (v if not pd.isna(v) else None) for k, v in r.items()},
                    "PartitionKey": str(r[cfg.partition_key_column]),
                    "RowKey": str(r[cfg.row_key_column]),
                }

            entities = [_entity(r) for r in records]
            entities.sort(key=itemgetter("PartitionKey"))

            for pk, group in groupby(entities, key=itemgetter("PartitionKey")):
                items = list(group)
                for chunk_start in range(0, len(items), cfg.batch_size):
                    chunk = items[chunk_start : chunk_start + cfg.batch_size]
                    if cfg.write_mode == "insert":
                        ops = [(TransactionOperation.CREATE, e) for e in chunk]
                    elif cfg.write_mode == "merge":
                        ops = [(TransactionOperation.UPDATE, e, {"mode": UpdateMode.MERGE}) for e in chunk]
                    else:  # upsert
                        ops = [(TransactionOperation.UPSERT, e, {"mode": UpdateMode.REPLACE}) for e in chunk]
                    try:
                        client.submit_transaction(ops)
                        sent += len(chunk)
                    except Exception as e:
                        errors.append(str(e)[:200])
                        context.log.warning(f"batch failed for partition '{pk}': {e}")

            context.log.info(f"Wrote {sent}/{len(records)} entities to {cfg.table_name}")
            return MaterializeResult(
                metadata={
                    "entities_written": MetadataValue.int(sent),
                    "table": MetadataValue.text(f"{account}/{cfg.table_name}"),
                    "write_mode": MetadataValue.text(cfg.write_mode),
                    "errors": MetadataValue.int(len(errors)),
                }
            )

        return Definitions(assets=[producer])
