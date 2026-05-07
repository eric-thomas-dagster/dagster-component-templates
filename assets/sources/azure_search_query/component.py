"""Azure AI Search query → DataFrame.

Run a search query against an Azure AI Search index and materialize the
results as a DataFrame asset. Supports keyword search, filters, semantic
search, and vector search.
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


class AzureSearchQueryComponent(Component, Model, Resolvable):
    """Query an Azure AI Search index and materialize results as a DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")

    endpoint: str = Field(description="Azure AI Search endpoint URL")
    index_name: str = Field(description="Index to query")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the query API key. Omit to use DefaultAzureCredential.",
    )

    search_text: str = Field(
        default="*",
        description="Search text (default '*' returns all docs subject to filter/top)",
    )
    filter_query: Optional[str] = Field(
        default=None,
        description="OData $filter expression, e.g. \"category eq 'Electronics' and price gt 100\"",
    )
    select: Optional[List[str]] = Field(
        default=None,
        description="Optional column projection — list of field names",
    )
    order_by: Optional[List[str]] = Field(
        default=None,
        description="Optional ordering, e.g. ['price desc', 'name asc']",
    )
    top: int = Field(default=1000, description="Max results to return (Azure max 1000 per request)")

    semantic_configuration_name: Optional[str] = Field(
        default=None,
        description="Optional semantic ranker configuration name (requires Standard tier and a configured semantic config)",
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
            group_name=self.group_name,
            description=self.description or f"Query Azure AI Search index '{self.index_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def search_query(context: AssetExecutionContext) -> MaterializeResult:
            try:
                from azure.search.documents import SearchClient
                from azure.core.credentials import AzureKeyCredential
                from azure.identity import DefaultAzureCredential
            except ImportError as e:
                raise ImportError(
                    "azure-search-documents + azure-identity required: "
                    "pip install azure-search-documents azure-identity"
                ) from e

            api_key = os.environ.get(cfg.api_key_env_var) if cfg.api_key_env_var else None
            cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()

            sc = SearchClient(endpoint=cfg.endpoint, index_name=cfg.index_name, credential=cred)

            kwargs = {"search_text": cfg.search_text, "top": cfg.top}
            if cfg.filter_query:
                kwargs["filter"] = cfg.filter_query
            if cfg.select:
                kwargs["select"] = cfg.select
            if cfg.order_by:
                kwargs["order_by"] = cfg.order_by
            if cfg.semantic_configuration_name:
                kwargs["query_type"] = "semantic"
                kwargs["semantic_configuration_name"] = cfg.semantic_configuration_name

            results = sc.search(**kwargs)
            rows = []
            for r in results:
                rows.append(dict(r))
            df = pd.DataFrame(rows)
            context.log.info(f"Search returned {len(df)} docs from {cfg.index_name}")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "index": MetadataValue.text(cfg.index_name),
                    "search_text": MetadataValue.text(cfg.search_text),
                    "filter": MetadataValue.text(cfg.filter_query or "(none)"),
                    "preview": MetadataValue.md(df.head(10).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[search_query])
