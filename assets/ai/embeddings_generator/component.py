"""Embeddings Generator Component.

Generate embeddings for text using multiple providers: OpenAI, Cohere, Sentence Transformers.
Supports batch processing, dimension reduction, and cosine similarity computation.
"""

import os
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class EmbeddingsGeneratorComponent(Component, Model, Resolvable):
    """Component for generating text embeddings for RAG and semantic search.

    This component generates vector embeddings for text using various providers.
    Embeddings can be used for semantic search, clustering, and RAG systems.

    Features:
    - Multiple providers: OpenAI, Cohere, Sentence Transformers
    - Batch processing for efficiency
    - Dimension reduction (PCA, UMAP)
    - Cosine similarity computation
    - Cost tracking (for API providers)
    - Normalization
    - Local model support (no API required)

    Use Cases:
    - RAG pipelines (embed document chunks)
    - Semantic search
    - Clustering and classification
    - Duplicate detection
    - Recommendation systems

    Example:
        ```yaml
        type: dagster_component_templates.EmbeddingsGeneratorComponent
        attributes:
          asset_name: document_embeddings
          provider: openai
          model: text-embedding-3-small
          api_key: "${OPENAI_API_KEY}"
          input_column: chunk
          output_column: embedding
          batch_size: 100
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the embeddings"
    )

    provider: str = Field(
        description="Embedding provider: openai, cohere, sentence_transformers, huggingface"
    )

    model: str = Field(
        description="Model name (e.g., 'text-embedding-3-small', 'embed-english-v3.0', 'all-MiniLM-L6-v2')"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for OpenAI/Cohere. Use ${API_KEY_NAME} for env vars."
    )

    input_column: str = Field(
        default="text",
        description="Column name containing text to embed"
    )

    output_column: str = Field(
        default="embedding",
        description="Column name for embeddings (list of floats)"
    )

    batch_size: int = Field(
        default=100,
        description="Batch size for API calls (larger = faster but more memory)"
    )

    normalize_embeddings: bool = Field(
        default=True,
        description="Normalize embeddings to unit length (recommended for cosine similarity)"
    )

    dimension_reduction: Optional[str] = Field(
        default=None,
        description="Reduce dimensions: 'pca', 'umap', or None"
    )

    target_dimensions: Optional[int] = Field(
        default=None,
        description="Target dimensions after reduction (e.g., 256)"
    )

    compute_similarity: bool = Field(
        default=False,
        description="Compute pairwise cosine similarity matrix"
    )

    similarity_threshold: Optional[float] = Field(
        default=None,
        description="If set, only store similarities above this threshold"
    )

    cache_embeddings: bool = Field(
        default=False,
        description="Cache embeddings to file for reuse"
    )

    cache_path: Optional[str] = Field(
        default=None,
        description="Path to cache embeddings (parquet format)"
    )

    rate_limit_delay: float = Field(
        default=0.0,
        description="Delay between batches in seconds (for API rate limits)"
    )

    track_costs: bool = Field(
        default=True,
        description="Track API costs (OpenAI, Cohere)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="embeddings",
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with text to embed")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        api_key = self.api_key
        input_column = self.input_column
        output_column = self.output_column
        batch_size = self.batch_size
        normalize_embeddings = self.normalize_embeddings
        dimension_reduction = self.dimension_reduction
        target_dimensions = self.target_dimensions
        compute_similarity = self.compute_similarity
        similarity_threshold = self.similarity_threshold
        cache_embeddings = self.cache_embeddings
        cache_path = self.cache_path
        rate_limit_delay = self.rate_limit_delay
        track_costs = self.track_costs
        description = self.description or f"Embeddings using {provider}/{model}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        upstream_asset_key = self.upstream_asset_key

        # Approximate costs per 1M tokens (as of 2024)
        COST_PER_1M_TOKENS = {
            "text-embedding-3-small": 0.02,
            "text-embedding-3-large": 0.13,
            "text-embedding-ada-002": 0.10,
            "embed-english-v3.0": 0.10,
            "embed-multilingual-v3.0": 0.10,
        }

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
        _comp_name = "embeddings_generator"  # component directory name
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
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def embeddings_generator_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that generates embeddings for text."""

            context.log.info(f"Starting embeddings generation with {provider}/{model}")

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows")

            # Validate input column
            if input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Check cache first
            if cache_embeddings and cache_path and os.path.exists(cache_path):
                context.log.info(f"Loading cached embeddings from {cache_path}")
                try:
                    cached_df = pd.read_parquet(cache_path)
                    if len(cached_df) == len(input_df) and input_column in cached_df.columns:
                        # Verify text matches
                        if (cached_df[input_column] == input_df[input_column]).all():
                            context.log.info("Cache hit! Using cached embeddings")
                            result_df = input_df.copy()
                            result_df[output_column] = cached_df[output_column]

                            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
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
                            return result_df
                except Exception as e:
                    context.log.warning(f"Failed to load cache: {e}, generating fresh")

            # Expand API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    raise ValueError(f"Environment variable not set: {var_name}")

            # Extract texts
            texts = input_df[input_column].astype(str).tolist()
            context.log.info(f"Generating embeddings for {len(texts)} texts")

            # Token tracking
            total_tokens = 0

            # Generate embeddings based on provider
            embeddings = []

            if provider == "openai":
                try:
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        response = client.embeddings.create(
                            model=model,
                            input=batch
                        )

                        batch_embeddings = [data.embedding for data in response.data]
                        embeddings.extend(batch_embeddings)

                        # Track tokens
                        total_tokens += response.usage.total_tokens

                        # Rate limiting
                        if rate_limit_delay > 0:
                            import time
                            time.sleep(rate_limit_delay)

                except ImportError:
                    raise ImportError("OpenAI package not installed. Install with: pip install openai")

            elif provider == "cohere":
                try:
                    import cohere
                    client = cohere.Client(api_key=expanded_api_key)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        response = client.embed(
                            texts=batch,
                            model=model,
                            input_type="search_document"
                        )

                        embeddings.extend(response.embeddings)

                        # Estimate tokens (rough approximation)
                        total_tokens += sum(len(t.split()) * 1.3 for t in batch)

                        # Rate limiting
                        if rate_limit_delay > 0:
                            import time
                            time.sleep(rate_limit_delay)

                except ImportError:
                    raise ImportError("Cohere package not installed. Install with: pip install cohere")

            elif provider == "sentence_transformers":
                try:
                    from sentence_transformers import SentenceTransformer
                    model_obj = SentenceTransformer(model)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        batch_embeddings = model_obj.encode(
                            batch,
                            normalize_embeddings=normalize_embeddings,
                            show_progress_bar=False,
                            batch_size=batch_size
                        )
                        embeddings.extend(batch_embeddings.tolist())

                except ImportError:
                    raise ImportError("sentence-transformers not installed. Install with: pip install sentence-transformers")

            elif provider == "huggingface":
                try:
                    from huggingface_hub import InferenceClient
                    client = InferenceClient(token=expanded_api_key)

                    for i, text in enumerate(texts):
                        if i % 10 == 0:
                            context.log.info(f"Processing text {i + 1}/{len(texts)}")

                        embedding = client.feature_extraction(text, model=model)
                        embeddings.append(embedding)

                        # Rate limiting
                        if rate_limit_delay > 0:
                            import time
                            time.sleep(rate_limit_delay)

                except ImportError:
                    raise ImportError("Hugging Face package not installed. Install with: pip install huggingface-hub")

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Generated {len(embeddings)} embeddings")

            # Convert to numpy array
            embeddings_array = np.array(embeddings)

            # Normalize if requested and not done by provider
            if normalize_embeddings and provider != "sentence_transformers":
                context.log.info("Normalizing embeddings to unit length")
                norms = np.linalg.norm(embeddings_array, axis=1, keepdims=True)
                embeddings_array = embeddings_array / norms

            # Dimension reduction if requested
            if dimension_reduction and target_dimensions:
                context.log.info(f"Reducing dimensions from {embeddings_array.shape[1]} to {target_dimensions}")

                if dimension_reduction == "pca":
                    try:
                        from sklearn.decomposition import PCA
                        pca = PCA(n_components=target_dimensions)
                        embeddings_array = pca.fit_transform(embeddings_array)
                        context.log.info(f"PCA explained variance: {pca.explained_variance_ratio_.sum():.2%}")
                    except ImportError:
                        raise ImportError("scikit-learn not installed. Install with: pip install scikit-learn")

                elif dimension_reduction == "umap":
                    try:
                        import umap
                        reducer = umap.UMAP(n_components=target_dimensions)
                        embeddings_array = reducer.fit_transform(embeddings_array)
                    except ImportError:
                        raise ImportError("umap-learn not installed. Install with: pip install umap-learn")

            # Convert back to list for DataFrame storage
            embeddings_list = embeddings_array.tolist()

            # Create result DataFrame
            result_df = input_df.copy()
            result_df[output_column] = embeddings_list

            context.log.info(f"Embeddings shape: {embeddings_array.shape}")

            # Compute similarity matrix if requested
            similarity_matrix = None
            if compute_similarity:
                context.log.info("Computing cosine similarity matrix")
                similarity_matrix = np.dot(embeddings_array, embeddings_array.T)

                # Apply threshold if set
                if similarity_threshold:
                    similarity_matrix[similarity_matrix < similarity_threshold] = 0

                # Add to result (as sparse representation or top-k for large datasets)
                if len(result_df) <= 1000:
                    # Store full matrix for small datasets
                    result_df['similarity_scores'] = [similarity_matrix[i].tolist() for i in range(len(result_df))]
                else:
                    # Store only top-k similar items for large datasets
                    context.log.info("Large dataset - storing top 10 similar items per row")
                    for i in range(len(result_df)):
                        scores = similarity_matrix[i]
                        top_k_indices = np.argsort(scores)[-11:-1][::-1]  # Top 10 excluding self
                        top_k_scores = scores[top_k_indices]
                        result_df.loc[i, 'top_similar_indices'] = [top_k_indices.tolist()]
                        result_df.loc[i, 'top_similar_scores'] = [top_k_scores.tolist()]

            # Cache embeddings if requested
            if cache_embeddings and cache_path:
                context.log.info(f"Caching embeddings to {cache_path}")
                os.makedirs(os.path.dirname(cache_path) if os.path.dirname(cache_path) else '.', exist_ok=True)
                cache_df = result_df[[input_column, output_column]].copy()
                cache_df.to_parquet(cache_path, index=False)

            # Calculate costs
            estimated_cost = 0.0
            if track_costs and total_tokens > 0:
                if model in COST_PER_1M_TOKENS:
                    estimated_cost = (total_tokens / 1_000_000) * COST_PER_1M_TOKENS[model]

            # Metadata
            embedding_dim = len(embeddings_list[0]) if embeddings_list else 0
            metadata = {
                "provider": provider,
                "model": model,
                "num_embeddings": len(embeddings_list),
                "embedding_dimension": embedding_dim,
                "normalized": normalize_embeddings,
            }

            if total_tokens > 0:
                metadata["total_tokens"] = int(total_tokens)
            if estimated_cost > 0:
                metadata["estimated_cost_usd"] = f"${estimated_cost:.4f}"
            if dimension_reduction:
                metadata["dimension_reduction"] = dimension_reduction
                metadata["original_dimension"] = embeddings_array.shape[1]
            if compute_similarity:
                metadata["similarity_computed"] = True

            if include_sample and len(result_df) > 0:
                # Show sample without full embedding arrays (too large)
                sample_df = result_df.head(10).copy()
                sample_df[output_column] = sample_df[output_column].apply(lambda x: f"[{len(x)} dims]")

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(sample_df.to_markdown()),
                        "preview": MetadataValue.dataframe(sample_df)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[embeddings_generator_asset])


        return Definitions(assets=[embeddings_generator_asset], asset_checks=list(_schema_checks))
