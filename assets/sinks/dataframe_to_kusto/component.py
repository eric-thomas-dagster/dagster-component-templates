"""DataFrame → Azure Data Explorer (Kusto).

Push a DataFrame to a Kusto table on an Azure Data Explorer cluster.
Uses queued ingestion by default (durable, retried by ADX) — set
`use_streaming_ingestion: true` for low-latency direct streaming
(requires the cluster to have streaming ingestion enabled).

Companion to `dataframe_from_kusto` — together they cover ADX
read/write workloads.
"""

import os
import tempfile
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


class DataframeToKustoComponent(Component, Model, Resolvable):
    """Push a DataFrame to a Kusto table via queued or streaming ingestion."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    cluster_url: str = Field(description="ADX cluster URL")
    database: str = Field(description="ADX database name")
    table: str = Field(description="Target Kusto table (must already exist with matching schema)")

    use_streaming_ingestion: bool = Field(
        default=False,
        description=(
            "If true, use streaming ingestion (low latency). Cluster must have "
            "streaming ingestion enabled. Otherwise uses queued ingestion (durable, "
            "default for batch workloads)."
        ),
    )
    if_exists: str = Field(
        default="append",
        description="'append' (default) | 'replace' (drops table contents first via .clear)",
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
        cfg = self
        kinds = self.kinds or ["azure", "kusto", "data_explorer"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Ingest DataFrame into Kusto table '{cfg.database}.{cfg.table}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def kusto_writer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
                from azure.kusto.ingest import (
                    QueuedIngestClient,
                    ManagedStreamingIngestClient,
                    IngestionProperties,
                    DataFormat,
                )
            except ImportError as e:
                raise ImportError(
                    "azure-kusto-data + azure-kusto-ingest required: "
                    "pip install azure-kusto-data azure-kusto-ingest"
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

            # Optionally clear the table first
            if cfg.if_exists == "replace":
                control = KustoClient(kcsb)
                control.execute(cfg.database, f".clear table {cfg.table} data")
                context.log.info(f"Cleared existing data in {cfg.database}.{cfg.table}")

            # Streaming or queued ingestion
            if cfg.use_streaming_ingestion:
                ingest_client = ManagedStreamingIngestClient.from_engine_kcsb(kcsb)
            else:
                # Queued ingestion uses the data-management endpoint (https://ingest-<cluster>...)
                ingest_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cfg.cluster_url.replace("https://", "https://ingest-"),
                    client_id, client_secret, tenant,
                ) if (tenant and client_id and client_secret) else (
                    KustoConnectionStringBuilder.with_az_cli_authentication(
                        cfg.cluster_url.replace("https://", "https://ingest-")
                    )
                )
                ingest_client = QueuedIngestClient(ingest_kcsb)

            ingestion_props = IngestionProperties(
                database=cfg.database,
                table=cfg.table,
                data_format=DataFormat.CSV,
            )

            # Write DataFrame to a temp CSV (Kusto SDK ingests files)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                upstream.to_csv(tmp.name, index=False, header=False)
                tmp_path = tmp.name

            try:
                ingest_client.ingest_from_file(tmp_path, ingestion_properties=ingestion_props)
                context.log.info(
                    f"Ingested {len(upstream)} rows into {cfg.database}.{cfg.table} "
                    f"({'streaming' if cfg.use_streaming_ingestion else 'queued'})"
                )
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

            return MaterializeResult(
                metadata={
                    "rows_ingested": MetadataValue.int(len(upstream)),
                    "cluster": MetadataValue.text(cfg.cluster_url),
                    "table": MetadataValue.text(f"{cfg.database}.{cfg.table}"),
                    "ingestion_mode": MetadataValue.text(
                        "streaming" if cfg.use_streaming_ingestion else "queued"
                    ),
                    "if_exists": MetadataValue.text(cfg.if_exists),
                }
            )

        return Definitions(assets=[kusto_writer])
