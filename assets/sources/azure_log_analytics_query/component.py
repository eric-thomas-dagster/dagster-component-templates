"""Azure Log Analytics — KQL query → DataFrame source.

Materialize a DataFrame asset from a KQL (Kusto Query Language) query
against an Azure Log Analytics workspace. Counterpart to the registry's
existing `audit_logs_to_sentinel` sink — this reads the same workspace
back as a queryable source for analytics, dashboards, ML, etc.

Common use cases:
- Pull operational metrics (sign-ins, errors, perf counters) from
  Application Insights / Azure Monitor / Sentinel into a DataFrame
  pipeline
- Build downstream analytics on log data (anomaly detection, cohort
  analysis, alerting on aggregated trends)
- Backfill from historical KQL queries (`ago(30d) | take 1000000`)

Auth: Microsoft Entra ID OAuth via DefaultAzureCredential. The principal
needs "Log Analytics Reader" role on the workspace (or a custom role
with `Microsoft.OperationalInsights/workspaces/query/read`).
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


class AzureLogAnalyticsQueryComponent(Component, Model, Resolvable):
    """Run a KQL query and materialize the result as a DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")
    workspace_id: str = Field(
        description="Log Analytics workspace ID (GUID, NOT the resource ID — find it in workspace overview)"
    )
    query: str = Field(
        description="KQL query to run. Example: 'AzureActivity | where TimeGenerated > ago(1h) | take 1000'"
    )
    timespan_iso8601: Optional[str] = Field(
        default=None,
        description=(
            "Optional time-range filter as ISO 8601 duration (e.g. 'PT1H' for 1h, 'P1D' for 1d, "
            "'P30D' for 30d). Acts as a query-level timespan parameter independent of the KQL itself."
        ),
    )

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

        asset_name = self.asset_name
        workspace_id = self.workspace_id
        query = self.query
        timespan = self.timespan_iso8601
        tenant_env = self.tenant_id_env_var
        client_env = self.client_id_env_var
        secret_env = self.client_secret_env_var

        kinds = self.kinds or ["azure", "kql", "log_analytics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            group_name=self.group_name,
            description=self.description or f"Materialize KQL query against Log Analytics workspace {workspace_id}.",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def query_asset(context: AssetExecutionContext) -> MaterializeResult:
            try:
                from azure.identity import DefaultAzureCredential, ClientSecretCredential
                from azure.monitor.query import LogsQueryClient, LogsQueryStatus
                from datetime import timedelta
                import isodate
            except ImportError as e:
                raise ImportError(
                    "azure-identity + azure-monitor-query + isodate required: "
                    "pip install azure-identity azure-monitor-query isodate"
                ) from e

            tenant = os.environ.get(tenant_env) if tenant_env else None
            client_id = os.environ.get(client_env) if client_env else None
            client_secret = os.environ.get(secret_env) if secret_env else None
            if tenant and client_id and client_secret:
                cred = ClientSecretCredential(tenant_id=tenant, client_id=client_id, client_secret=client_secret)
            else:
                cred = DefaultAzureCredential()

            client = LogsQueryClient(cred)
            ts = isodate.parse_duration(timespan) if timespan else None

            context.log.info(f"Running KQL query against workspace {workspace_id}")
            response = client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=ts,
            )

            if response.status == LogsQueryStatus.PARTIAL:
                context.log.warning(f"Partial result: {getattr(response, 'partial_error', 'unknown error')}")

            tables = getattr(response, "tables", None) or getattr(response, "partial_data", [])
            if not tables:
                context.log.info("Query returned no tables")
                df = pd.DataFrame()
            else:
                # First table is the primary result
                t = tables[0]
                df = pd.DataFrame(t.rows, columns=t.columns)
            context.log.info(f"Returned {len(df)} rows × {len(df.columns)} columns")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "columns": MetadataValue.text(", ".join(df.columns.tolist()) if len(df.columns) else "(no columns)"),
                    "workspace_id": MetadataValue.text(workspace_id),
                    "query": MetadataValue.text(query[:500]),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[query_asset])
