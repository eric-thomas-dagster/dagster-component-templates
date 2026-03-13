"""pgvector Reader Component.

Query a PostgreSQL pgvector table for similar vectors, returning results as
a DataFrame. Supports cosine, L2, and inner product distance metrics.
"""

import os
from dataclasses import dataclass
from typing import Optional, List
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


class PgvectorReaderComponent(Component, Model, Resolvable):
    """Component for querying a pgvector table for similar vectors.

    Performs approximate nearest-neighbor search against a PostgreSQL table
    with pgvector extension. Optionally embeds query text via OpenAI.

    Example:
        ```yaml
        type: dagster_component_templates.PgvectorReaderComponent
        attributes:
          asset_name: similar_documents
          table_name: document_embeddings
          query_text: "machine learning fundamentals"
          n_results: 20
          distance_metric: cosine
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    query_text: Optional[str] = Field(
        default=None,
        description="Text to search for (will be embedded using OpenAI)"
    )

    query_embedding: Optional[List[float]] = Field(
        default=None,
        description="Pre-computed embedding vector for search"
    )

    database_url_env_var: str = Field(
        default="DATABASE_URL",
        description="Environment variable containing the PostgreSQL connection URL"
    )

    table_name: str = Field(
        description="pgvector table to query"
    )

    embedding_column: str = Field(
        default="embedding",
        description="Column containing vector embeddings"
    )

    n_results: int = Field(
        default=10,
        description="Number of similar results to return"
    )

    distance_metric: str = Field(
        default="cosine",
        description="Distance metric: 'cosine' (<=>), 'l2' (<->), 'inner_product' (<#>)"
    )

    embedding_model: Optional[str] = Field(
        default=None,
        description="OpenAI embedding model for embedding query_text (default: text-embedding-3-small)"
    )

    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable containing OpenAI API key"
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage (does not load data)"
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
        query_text = self.query_text
        query_embedding = self.query_embedding
        database_url_env_var = self.database_url_env_var
        table_name = self.table_name
        embedding_column = self.embedding_column
        n_results = self.n_results
        distance_metric = self.distance_metric
        embedding_model = self.embedding_model
        api_key_env_var = self.api_key_env_var
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
            description=f"pgvector similarity search on {table_name}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def pgvector_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query pgvector table for similar vectors."""
            try:
                import sqlalchemy
            except ImportError:
                raise ImportError("sqlalchemy is required: pip install sqlalchemy psycopg2-binary pgvector")

            engine = sqlalchemy.create_engine(os.environ[database_url_env_var])

            vec = query_embedding
            if query_text and not vec:
                try:
                    from openai import OpenAI
                except ImportError:
                    raise ImportError("openai is required for text embedding: pip install openai")
                client = OpenAI(api_key=os.environ.get(api_key_env_var))
                resp = client.embeddings.create(
                    input=query_text,
                    model=embedding_model or "text-embedding-3-small"
                )
                vec = resp.data[0].embedding

            if vec is None:
                raise ValueError("Either query_text or query_embedding must be provided")

            op_map = {
                "cosine": "<=>",
                "l2": "<->",
                "inner_product": "<#>",
            }
            op = op_map.get(distance_metric, "<=>")

            sql = (
                f"SELECT *, {embedding_column} {op} '{vec}'::vector AS distance "
                f"FROM {table_name} ORDER BY distance LIMIT {n_results}"
            )

            context.log.info(f"Querying {table_name} for top {n_results} results using {distance_metric} distance")
            df = pd.read_sql(sql, engine)

            context.log.info(f"Retrieved {len(df)} results")
            context.add_output_metadata({
                "n_results": MetadataValue.int(len(df)),
                "table_name": MetadataValue.text(table_name),
                "distance_metric": MetadataValue.text(distance_metric),
            })
            return df

        return Definitions(assets=[pgvector_reader_asset])
