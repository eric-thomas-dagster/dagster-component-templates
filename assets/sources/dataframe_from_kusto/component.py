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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["azure", "kusto", "data_explorer"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"KQL query against ADX cluster {cfg.cluster_url}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
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
