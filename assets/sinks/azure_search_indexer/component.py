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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
