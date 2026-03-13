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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
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

        # Infer kinds from component name if not explicitly set
        _comp_name = "elasticsearch_reader"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            description=f"Elasticsearch query on index {index_name}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
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


            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[elasticsearch_reader_asset])


        return Definitions(assets=[elasticsearch_reader_asset], asset_checks=list(_schema_checks))
