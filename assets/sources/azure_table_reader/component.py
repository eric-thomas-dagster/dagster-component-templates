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
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Read entities from Azure Table '{self.table_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
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
