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
from pydantic import AliasChoices, Field


class DataframeToFabricLakehouseComponent(Component, Model, Resolvable):
    """Write a DataFrame to a Microsoft Fabric Lakehouse table (Delta on OneLake)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse name (the .Lakehouse item in the workspace)")
    table: str = Field(
        description="Target Delta table name (created if missing)",
        validation_alias=AliasChoices("table", "table_name"),
    )
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

        asset_name = self.asset_name
        upstream = self.upstream_asset_key
        ws = self.workspace_id
        lh = self.lakehouse_name
        table = self.table
        write_mode = self.write_mode
        tenant_env = self.tenant_id_env_var
        client_env = self.client_id_env_var
        secret_env = self.client_secret_env_var

        kinds = ["azure", "fabric", "delta"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream))},
            group_name=self.group_name,
            description=self.description or f"Write DataFrame to Fabric Lakehouse '{lh}' table '{table}' (Delta).",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
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
