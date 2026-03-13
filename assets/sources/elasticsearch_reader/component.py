"""Elasticsearch Reader Component.

Search and query an Elasticsearch index, returning matching documents as a
DataFrame. Supports query DSL, multi-match text search, and field filtering.
"""

import os
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class ElasticsearchReaderComponent(Component, Model, Resolvable):
    """Component for searching and querying an Elasticsearch index.

    Executes queries against an Elasticsearch index using either query DSL
    or simple text search, returning results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.ElasticsearchReaderComponent
        attributes:
          asset_name: es_search_results
          index_name: products
          search_text: "wireless headphones"
          search_fields: ["title", "description"]
          n_results: 50
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    index_name: str = Field(description="Elasticsearch index to query")

    host_env_var: str = Field(
        default="ELASTICSEARCH_URL",
        description="Environment variable with Elasticsearch URL (e.g., http://localhost:9200)"
    )

    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing Elasticsearch API key (optional)"
    )

    query: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Elasticsearch query DSL (None = match_all)"
    )

    search_text: Optional[str] = Field(
        default=None,
        description="Simple text search string (uses multi_match query)"
    )

    search_fields: Optional[List[str]] = Field(
        default=None,
        description="Fields to search in for text search (None = all fields)"
    )

    n_results: int = Field(
        default=100,
        description="Maximum number of documents to return"
    )

    source_fields: Optional[List[str]] = Field(
        default=None,
        description="Fields to return in results (None = all fields)"
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        index_name = self.index_name
        host_env_var = self.host_env_var
        api_key_env_var = self.api_key_env_var
        query = self.query
        search_text = self.search_text
        search_fields = self.search_fields
        n_results = self.n_results
        source_fields = self.source_fields
        deps = self.deps
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            description=f"Elasticsearch query on index {index_name}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def elasticsearch_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query Elasticsearch index and return results as DataFrame."""
            try:
                from elasticsearch import Elasticsearch
            except ImportError:
                raise ImportError("elasticsearch is required: pip install elasticsearch")

            es_kwargs: Dict[str, Any] = {"hosts": [os.environ[host_env_var]]}
            if api_key_env_var:
                es_kwargs["api_key"] = os.environ[api_key_env_var]

            es = Elasticsearch(**es_kwargs)

            if search_text:
                q: Dict[str, Any] = {
                    "query": {
                        "multi_match": {
                            "query": search_text,
                            "fields": search_fields or ["*"],
                        }
                    }
                }
            else:
                q = query or {"query": {"match_all": {}}}

            context.log.info(f"Querying Elasticsearch index '{index_name}' for up to {n_results} results")

            resp = es.search(
                index=index_name,
                body=q,
                size=n_results,
                source=source_fields,
            )

            rows = [
                {"_id": hit["_id"], "_score": hit.get("_score"), **hit["_source"]}
                for hit in resp["hits"]["hits"]
            ]
            df = pd.DataFrame(rows)

            total_hits = resp["hits"]["total"]["value"] if isinstance(resp["hits"]["total"], dict) else resp["hits"]["total"]
            context.log.info(f"Retrieved {len(df)} documents (total matches: {total_hits})")

            context.add_output_metadata({
                "n_results": MetadataValue.int(len(df)),
                "total_hits": MetadataValue.int(int(total_hits)),
                "index_name": MetadataValue.text(index_name),
            })
            return df

        return Definitions(assets=[elasticsearch_reader_asset])
