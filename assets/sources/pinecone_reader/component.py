"""Pinecone Reader Component.

Query a Pinecone vector index for similar vectors, returning results as a
DataFrame with id, score, and metadata columns.
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


class PineconeReaderComponent(Component, Model, Resolvable):
    """Component for querying a Pinecone vector index.

    Performs similarity search against a Pinecone index and returns results
    as a DataFrame. Optionally embeds query text via OpenAI.

    Example:
        ```yaml
        type: dagster_component_templates.PineconeReaderComponent
        attributes:
          asset_name: pinecone_results
          index_name: my-embeddings
          query_text: "product recommendations"
          n_results: 10
          include_metadata: true
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    index_name: str = Field(description="Pinecone index to query")

    api_key_env_var: str = Field(
        default="PINECONE_API_KEY",
        description="Environment variable containing Pinecone API key"
    )

    environment_env_var: str = Field(
        default="PINECONE_ENVIRONMENT",
        description="Environment variable containing Pinecone environment"
    )

    query_vector: Optional[List[float]] = Field(
        default=None,
        description="Pre-computed query vector for search"
    )

    query_text: Optional[str] = Field(
        default=None,
        description="Text to embed and search (requires OpenAI)"
    )

    embedding_model: Optional[str] = Field(
        default=None,
        description="OpenAI embedding model (default: text-embedding-3-small)"
    )

    openai_api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable containing OpenAI API key"
    )

    n_results: int = Field(
        default=10,
        description="Number of similar results to return"
    )

    namespace: Optional[str] = Field(
        default=None,
        description="Pinecone namespace to query"
    )

    filter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metadata filter for query (e.g., {\"category\": \"electronics\"})"
    )

    include_values: bool = Field(
        default=False,
        description="Include vector values in results"
    )

    include_metadata: bool = Field(
        default=True,
        description="Include metadata in results"
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
        api_key_env_var = self.api_key_env_var
        query_vector = self.query_vector
        query_text = self.query_text
        embedding_model = self.embedding_model
        openai_api_key_env_var = self.openai_api_key_env_var
        n_results = self.n_results
        namespace = self.namespace
        filter_ = self.filter
        include_values = self.include_values
        include_metadata = self.include_metadata
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
        _comp_name = "pinecone_reader"  # component directory name
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
            description=f"Pinecone similarity search on index {index_name}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def pinecone_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query Pinecone index for similar vectors."""
            try:
                from pinecone import Pinecone
            except ImportError:
                raise ImportError("pinecone-client is required: pip install pinecone-client")

            pc = Pinecone(api_key=os.environ[api_key_env_var])
            index = pc.Index(index_name)

            vec = query_vector
            if query_text and not vec:
                try:
                    from openai import OpenAI
                except ImportError:
                    raise ImportError("openai is required for text embedding: pip install openai")
                client = OpenAI(api_key=os.environ.get(openai_api_key_env_var))
                resp = client.embeddings.create(
                    input=query_text,
                    model=embedding_model or "text-embedding-3-small"
                )
                vec = resp.data[0].embedding

            if vec is None:
                raise ValueError("Either query_text or query_vector must be provided")

            context.log.info(f"Querying Pinecone index '{index_name}' for top {n_results} results")

            results = index.query(
                vector=vec,
                top_k=n_results,
                namespace=namespace,
                filter=filter_,
                include_values=include_values,
                include_metadata=include_metadata,
            )

            rows = [
                {"id": m.id, "score": m.score, **(m.metadata or {})}
                for m in results.matches
            ]
            df = pd.DataFrame(rows)

            context.log.info(f"Retrieved {len(df)} results from Pinecone")

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
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
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


        _schema_checks = build_column_schema_change_checks(assets=[pinecone_reader_asset])


        return Definitions(assets=[pinecone_reader_asset], asset_checks=list(_schema_checks))
