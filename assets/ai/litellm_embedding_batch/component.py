"""LiteLLM Embedding Batch Component.

Generate embeddings for a text column using LiteLLM with automatic
model routing and fallback. Processes rows in configurable batches.
"""
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
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


@dataclass
class LitellmEmbeddingBatchComponent(Component, Model, Resolvable):
    """Generate embeddings for a text column using LiteLLM with batching and fallback.

    Processes texts in configurable batches for efficient API usage.
    Writes embedding vectors (lists of floats) to the output column.

    Example:
        ```yaml
        type: dagster_component_templates.LitellmEmbeddingBatchComponent
        attributes:
          asset_name: document_embeddings
          upstream_asset_key: processed_documents
          text_column: content
          model: text-embedding-3-small
          batch_size: 100
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing input text")
    output_column: str = Field(default="embedding", description="Column to write embedding vectors (lists of floats)")
    model: str = Field(default="text-embedding-3-small", description='Embedding model (e.g. "text-embedding-3-small", "text-embedding-ada-002", "cohere/embed-english-v3.0")')
    batch_size: int = Field(default=100, description="Rows per API call")
    dimensions: Optional[int] = Field(default=None, description="Embedding dimensions (for models that support it)")
    fallback_models: Optional[List[str]] = Field(default=None, description="Models to try if primary fails")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var name for API key")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        text_column = self.text_column
        output_column = self.output_column
        model = self.model
        batch_size = self.batch_size
        dimensions = self.dimensions
        fallback_models = self.fallback_models
        api_key_env_var = self.api_key_env_var
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            kinds={"ai", "embeddings"},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os

            df = upstream.copy()
            context.log.info(f"Generating embeddings for {len(df)} rows with model={model}, batch_size={batch_size}")

            kwargs: dict = {"model": model}
            if dimensions:
                kwargs["dimensions"] = dimensions
            if fallback_models:
                kwargs["fallbacks"] = fallback_models
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            texts = df[text_column].astype(str).tolist()
            all_embeddings = []

            for batch_start in range(0, len(texts), batch_size):
                batch = texts[batch_start: batch_start + batch_size]
                response = litellm.embedding(input=batch, **kwargs                api_key=api_key,
            )
                batch_embeddings = [item["embedding"] for item in response.data]
                all_embeddings.extend(batch_embeddings)
                context.log.info(f"Embedded {min(batch_start + batch_size, len(texts))}/{len(texts)} rows")

            df[output_column] = all_embeddings

            embedding_dim = len(all_embeddings[0]) if all_embeddings else 0
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "model": MetadataValue.text(model),
                "embedding_dimensions": MetadataValue.int(embedding_dim),
                "output_column": MetadataValue.text(output_column),
            })
            return df

        return Definitions(assets=[_asset])
