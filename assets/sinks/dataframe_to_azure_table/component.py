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
from pydantic import AliasChoices, Field


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
    table: str = Field(
        description="Table name (created if it doesn't exist)",
        validation_alias=AliasChoices("table", "table_name"),
    )

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

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        cfg = self
        kinds = self.kinds or ["azure", "table_storage"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Write DataFrame rows to Azure Table '{self.table}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
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
                    service.create_table(cfg.table)
                    context.log.info(f"Created table '{cfg.table}'")
                except Exception:
                    pass  # exists already

            client = service.get_table_client(cfg.table)

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

            context.log.info(f"Wrote {sent}/{len(records)} entities to {cfg.table}")
            return MaterializeResult(
                metadata={
                    "entities_written": MetadataValue.int(sent),
                    "table": MetadataValue.text(f"{account}/{cfg.table}"),
                    "write_mode": MetadataValue.text(cfg.write_mode),
                    "errors": MetadataValue.int(len(errors)),
                }
            )

        return Definitions(assets=[producer])
