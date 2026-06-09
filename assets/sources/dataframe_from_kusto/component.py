"""Azure Data Explorer (Kusto) → DataFrame.

Run a KQL query against an Azure Data Explorer (ADX) cluster and
materialize the result as a DataFrame asset.

ADX vs Log Analytics:
- Both speak KQL
- Log Analytics is a managed service with built-in retention/ingestion
  policies, optimized for Azure operational logs (Sentinel, AppInsights,
  AzureActivity)
- ADX is a raw Kusto cluster you provision yourself for high-volume
  custom telemetry (security analytics, IoT, app logs at scale,
  near-real-time analytics)

For Log Analytics queries use `azure_log_analytics_query`; for ADX
clusters use this component (different SDK: azure-kusto-data).

Auth: Microsoft Entra OAuth via DefaultAzureCredential. Principal needs
the appropriate role on the cluster + database (Database User / Viewer
at minimum for queries).
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


class DataframeFromKustoComponent(Component, Model, Resolvable):
    """Run a KQL query against ADX and materialize as a DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")

    cluster_url: str = Field(
        description="ADX cluster URL, e.g. 'https://mycluster.eastus.kusto.windows.net'"
    )
    database: str = Field(description="ADX database name")
    query: str = Field(description="KQL query to execute")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

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
        kinds = self.kinds or ["azure", "kusto", "data_explorer"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or f"KQL query against ADX cluster {cfg.cluster_url}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def kusto_query(context: AssetExecutionContext) -> MaterializeResult:
            try:
                from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
                from azure.kusto.data.helpers import dataframe_from_result_table
            except ImportError as e:
                raise ImportError(
                    "azure-kusto-data required: pip install azure-kusto-data"
                ) from e

            tenant = os.environ.get(cfg.tenant_id_env_var) if cfg.tenant_id_env_var else None
            client_id = os.environ.get(cfg.client_id_env_var) if cfg.client_id_env_var else None
            client_secret = os.environ.get(cfg.client_secret_env_var) if cfg.client_secret_env_var else None

            if tenant and client_id and client_secret:
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cfg.cluster_url, client_id, client_secret, tenant
                )
            else:
                kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cfg.cluster_url)

            client = KustoClient(kcsb)
            context.log.info(f"Running KQL on {cfg.cluster_url} / {cfg.database}")
            response = client.execute(cfg.database, cfg.query)
            df = dataframe_from_result_table(response.primary_results[0])
            context.log.info(f"Returned {len(df)} rows × {len(df.columns)} columns")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "columns": MetadataValue.text(", ".join(df.columns.tolist()) if len(df.columns) else "(empty)"),
                    "cluster": MetadataValue.text(cfg.cluster_url),
                    "database": MetadataValue.text(cfg.database),
                    "query": MetadataValue.text(cfg.query[:500]),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[kusto_query])
