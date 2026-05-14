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
            group_name=self.group_name,
            description=self.description or f"Query Azure AI Search index '{self.index_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
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
