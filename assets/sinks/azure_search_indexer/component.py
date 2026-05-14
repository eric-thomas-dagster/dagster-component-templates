"""DataFrame → Azure AI Search index.

Push DataFrame rows as documents to an Azure AI Search index. Supports
text search, vector search, and hybrid retrieval — the backbone of
RAG, semantic search, and enterprise knowledge bases on Azure.

Each row becomes one search document. The DataFrame must include a
column matching the index's `key` field (by default `id`).

Auth: API key (default) or Microsoft Entra OAuth via DefaultAzureCredential.
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


class AzureSearchIndexerComponent(Component, Model, Resolvable):
    """Push DataFrame rows to an Azure AI Search index as documents."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    endpoint: str = Field(
        description="Azure AI Search endpoint URL, e.g. 'https://my-search.search.windows.net'"
    )
    index_name: str = Field(description="Target search index (must already exist)")

    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding an admin API key. Omit to use DefaultAzureCredential (Entra OAuth).",
    )
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    action: str = Field(
        default="mergeOrUpload",
        description="'upload' (insert/replace), 'merge' (partial update), 'mergeOrUpload', or 'delete'",
    )
    batch_size: int = Field(
        default=1000,
        ge=1,
        le=1000,
        description="Max documents per batch (Azure caps at 1000)",
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
        kinds = self.kinds or ["azure", "ai_search"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Push DataFrame rows to Azure AI Search index '{self.index_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def indexer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.search.documents import SearchClient
                from azure.core.credentials import AzureKeyCredential
                from azure.identity import DefaultAzureCredential, ClientSecretCredential
            except ImportError as e:
                raise ImportError(
                    "azure-search-documents + azure-identity required: "
                    "pip install azure-search-documents azure-identity"
                ) from e

            api_key = os.environ.get(cfg.api_key_env_var) if cfg.api_key_env_var else None
            if api_key:
                cred = AzureKeyCredential(api_key)
            else:
                tenant = os.environ.get(cfg.tenant_id_env_var) if cfg.tenant_id_env_var else None
                client = os.environ.get(cfg.client_id_env_var) if cfg.client_id_env_var else None
                secret = os.environ.get(cfg.client_secret_env_var) if cfg.client_secret_env_var else None
                if tenant and client and secret:
                    cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
                else:
                    cred = DefaultAzureCredential()

            sc = SearchClient(endpoint=cfg.endpoint, index_name=cfg.index_name, credential=cred)

            docs = upstream.to_dict(orient="records")
            sent, failed = 0, 0
            for chunk_start in range(0, len(docs), cfg.batch_size):
                chunk = docs[chunk_start : chunk_start + cfg.batch_size]
                if cfg.action == "upload":
                    results = sc.upload_documents(chunk)
                elif cfg.action == "merge":
                    results = sc.merge_documents(chunk)
                elif cfg.action == "delete":
                    results = sc.delete_documents(chunk)
                else:
                    results = sc.merge_or_upload_documents(chunk)
                for r in results:
                    if r.succeeded:
                        sent += 1
                    else:
                        failed += 1
                        context.log.warning(f"doc {r.key} failed: {r.error_message}")

            context.log.info(f"Indexed {sent} docs ({failed} failed) into {cfg.index_name}")
            return MaterializeResult(
                metadata={
                    "documents_indexed": MetadataValue.int(sent),
                    "documents_failed": MetadataValue.int(failed),
                    "index": MetadataValue.text(cfg.index_name),
                    "action": MetadataValue.text(cfg.action),
                }
            )

        return Definitions(assets=[indexer])
