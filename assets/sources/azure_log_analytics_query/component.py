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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
