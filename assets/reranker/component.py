"""Reranker Component.

Rerank search results for improved relevance using Cohere Rerank API,
cross-encoder models, and BM25 algorithms.
"""

import os
import time
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np

from dagster import (
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


class RerankerComponent(Component, Model, Resolvable):
    """Component for reranking search results to improve relevance.

    This component implements two-stage retrieval by reranking initial search results.
    It supports multiple reranking strategies including Cohere's Rerank API, cross-encoder
    models from sentence-transformers, and BM25 for keyword-based reranking.

    Features:
    - Cohere Rerank API integration (state-of-the-art)
    - Cross-encoder models (sentence-transformers)
    - BM25 algorithm for keyword matching
    - Batch reranking support
    - Relevance score normalization
    - Top-N filtering
    - Multiple query support
    - Cost tracking (for Cohere API)

    Use Cases:
    - RAG pipeline optimization (improve retrieval accuracy)
    - Semantic search enhancement
    - Question-answering systems
    - Document ranking
    - Hybrid search (combine semantic + keyword)

    Example:
        ```yaml
        type: dagster_component_templates.RerankerComponent
        attributes:
          asset_name: reranked_search_results
          source_asset: vector_search_results
          method: cohere
          model: rerank-english-v2.0
          query_column: query
          text_column: document
          top_n: 10
          api_key: "${COHERE_API_KEY}"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold reranked results"
    )

    source_asset: str = Field(
        description="Name of upstream asset containing search results"
    )

    method: str = Field(
        description="Reranking method: cohere, cross_encoder, bm25"
    )

    model: Optional[str] = Field(
        default=None,
        description="Model name (e.g., 'rerank-english-v2.0', 'ms-marco-MiniLM-L-12-v2')"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for Cohere. Use ${COHERE_API_KEY} for env vars."
    )

    query_column: str = Field(
        default="query",
        description="Column name containing search queries"
    )

    text_column: str = Field(
        default="text",
        description="Column name containing document text to rerank"
    )

    score_column: Optional[str] = Field(
        default="score",
        description="Column name for original scores (optional)"
    )

    output_score_column: str = Field(
        default="rerank_score",
        description="Column name for reranking scores"
    )

    top_n: Optional[int] = Field(
        default=None,
        description="Return only top N results after reranking (None = all)"
    )

    rerank_threshold: Optional[float] = Field(
        default=None,
        description="Minimum rerank score threshold (0.0-1.0)"
    )

    batch_size: int = Field(
        default=100,
        description="Batch size for reranking (for cross-encoder)"
    )

    normalize_scores: bool = Field(
        default=True,
        description="Normalize scores to 0-1 range"
    )

    combine_scores: Optional[str] = Field(
        default=None,
        description="Combine with original scores: 'weighted', 'multiply', or None"
    )

    score_weight: float = Field(
        default=0.5,
        description="Weight for combining scores (0.0-1.0). 0.5 = equal weight."
    )

    return_documents: bool = Field(
        default=True,
        description="Return document text in results"
    )

    max_chunks_per_doc: Optional[int] = Field(
        default=None,
        description="Max chunks per document to rerank (for large results)"
    )

    rate_limit_delay: float = Field(
        default=0.0,
        description="Delay between API calls in seconds"
    )

    track_costs: bool = Field(
        default=True,
        description="Track API costs (for Cohere)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="reranking",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        method = self.method
        model = self.model
        api_key = self.api_key
        query_column = self.query_column
        text_column = self.text_column
        score_column = self.score_column
        output_score_column = self.output_score_column
        top_n = self.top_n
        rerank_threshold = self.rerank_threshold
        batch_size = self.batch_size
        normalize_scores = self.normalize_scores
        combine_scores = self.combine_scores
        score_weight = self.score_weight
        return_documents = self.return_documents
        max_chunks_per_doc = self.max_chunks_per_doc
        rate_limit_delay = self.rate_limit_delay
        track_costs = self.track_costs
        description = self.description or f"Reranked results using {method}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cohere Rerank pricing (approximate, as of 2024)
        COHERE_RERANK_COST_PER_1K_SEARCHES = 0.02  # $0.02 per 1000 searches

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def reranker_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that reranks search results for improved relevance."""

            context.log.info(f"Starting reranking with method: {method}")

            # Get input DataFrame from upstream assets
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Reranker requires an upstream DataFrame with search results")

            # Validate columns
            if query_column not in input_df.columns:
                raise ValueError(f"Query column '{query_column}' not found. Available: {list(input_df.columns)}")
            if text_column not in input_df.columns:
                raise ValueError(f"Text column '{text_column}' not found. Available: {list(input_df.columns)}")

            # Expand API key if provided
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    raise ValueError(f"Environment variable not set: {var_name}")

            # Extract queries and documents
            queries = input_df[query_column].tolist()
            documents = input_df[text_column].astype(str).tolist()
            original_scores = input_df[score_column].tolist() if score_column and score_column in input_df.columns else None

            context.log.info(f"Reranking {len(documents)} documents for {len(set(queries))} unique queries")

            # Apply max chunks limit if specified
            if max_chunks_per_doc and len(documents) > max_chunks_per_doc:
                context.log.warning(f"Limiting to {max_chunks_per_doc} chunks (from {len(documents)})")
                input_df = input_df.head(max_chunks_per_doc)
                queries = queries[:max_chunks_per_doc]
                documents = documents[:max_chunks_per_doc]
                if original_scores:
                    original_scores = original_scores[:max_chunks_per_doc]

            # Track metrics
            rerank_scores = []
            total_api_calls = 0
            start_time = time.time()

            # Rerank based on method
            if method == "cohere":
                if not expanded_api_key:
                    raise ValueError("Cohere reranking requires api_key")

                try:
                    import cohere
                    client = cohere.Client(api_key=expanded_api_key)

                    # Default model
                    rerank_model = model or "rerank-english-v2.0"
                    context.log.info(f"Using Cohere model: {rerank_model}")

                    # Group by query for batch reranking
                    query_groups = {}
                    for idx, (query, doc) in enumerate(zip(queries, documents)):
                        if query not in query_groups:
                            query_groups[query] = []
                        query_groups[query].append((idx, doc))

                    # Initialize scores array
                    rerank_scores = [0.0] * len(documents)

                    # Rerank each query group
                    for query, doc_list in query_groups.items():
                        indices = [idx for idx, doc in doc_list]
                        docs = [doc for idx, doc in doc_list]

                        context.log.info(f"Reranking {len(docs)} documents for query: '{query[:50]}...'")

                        # Call Cohere Rerank API
                        response = client.rerank(
                            model=rerank_model,
                            query=query,
                            documents=docs,
                            top_n=top_n or len(docs),
                            return_documents=return_documents
                        )

                        total_api_calls += 1

                        # Extract scores and map back to original indices
                        for result in response.results:
                            original_idx = indices[result.index]
                            rerank_scores[original_idx] = result.relevance_score

                        # Rate limiting
                        if rate_limit_delay > 0:
                            time.sleep(rate_limit_delay)

                    context.log.info(f"Completed {total_api_calls} Cohere API calls")

                except ImportError:
                    raise ImportError("Cohere package not installed. Install with: pip install cohere")

            elif method == "cross_encoder":
                try:
                    from sentence_transformers import CrossEncoder

                    # Default model
                    cross_encoder_model = model or "cross-encoder/ms-marco-MiniLM-L-12-v2"
                    context.log.info(f"Loading cross-encoder model: {cross_encoder_model}")

                    model_obj = CrossEncoder(cross_encoder_model)

                    # Group by query
                    query_groups = {}
                    for idx, (query, doc) in enumerate(zip(queries, documents)):
                        if query not in query_groups:
                            query_groups[query] = []
                        query_groups[query].append((idx, query, doc))

                    # Initialize scores array
                    rerank_scores = [0.0] * len(documents)

                    # Rerank each query group
                    for query, doc_list in query_groups.items():
                        indices = [idx for idx, q, doc in doc_list]
                        pairs = [(q, doc) for idx, q, doc in doc_list]

                        context.log.info(f"Reranking {len(pairs)} documents for query: '{query[:50]}...'")

                        # Batch predict
                        scores = model_obj.predict(pairs, batch_size=batch_size, show_progress_bar=False)

                        # Map scores back
                        for i, score in enumerate(scores):
                            rerank_scores[indices[i]] = float(score)

                except ImportError:
                    raise ImportError("sentence-transformers not installed. Install with: pip install sentence-transformers")

            elif method == "bm25":
                try:
                    from rank_bm25 import BM25Okapi

                    context.log.info("Using BM25 for reranking")

                    # Tokenize documents
                    tokenized_docs = [doc.lower().split() for doc in documents]

                    # Build BM25 index
                    bm25 = BM25Okapi(tokenized_docs)

                    # Group by query
                    query_groups = {}
                    for idx, query in enumerate(queries):
                        if query not in query_groups:
                            query_groups[query] = []
                        query_groups[query].append(idx)

                    # Initialize scores array
                    rerank_scores = [0.0] * len(documents)

                    # Score each query
                    for query, indices in query_groups.items():
                        tokenized_query = query.lower().split()
                        scores = bm25.get_scores(tokenized_query)

                        for idx in indices:
                            rerank_scores[idx] = float(scores[idx])

                except ImportError:
                    raise ImportError("rank-bm25 not installed. Install with: pip install rank-bm25")

            else:
                raise ValueError(f"Unsupported reranking method: {method}")

            # Normalize scores if requested
            if normalize_scores and rerank_scores:
                min_score = min(rerank_scores)
                max_score = max(rerank_scores)
                if max_score > min_score:
                    rerank_scores = [(s - min_score) / (max_score - min_score) for s in rerank_scores]
                    context.log.info("Normalized rerank scores to 0-1 range")

            # Combine with original scores if requested
            final_scores = rerank_scores
            if combine_scores and original_scores:
                if combine_scores == "weighted":
                    final_scores = [
                        score_weight * rerank_scores[i] + (1 - score_weight) * original_scores[i]
                        for i in range(len(rerank_scores))
                    ]
                    context.log.info(f"Combined scores with weight {score_weight}")
                elif combine_scores == "multiply":
                    final_scores = [
                        rerank_scores[i] * original_scores[i]
                        for i in range(len(rerank_scores))
                    ]
                    context.log.info("Multiplied rerank scores with original scores")

            # Add scores to DataFrame
            result_df = input_df.copy()
            result_df[output_score_column] = final_scores

            # Apply threshold filter
            if rerank_threshold is not None:
                before_count = len(result_df)
                result_df = result_df[result_df[output_score_column] >= rerank_threshold]
                context.log.info(f"Filtered by threshold {rerank_threshold}: {before_count} -> {len(result_df)} results")

            # Sort by rerank score
            result_df = result_df.sort_values(by=output_score_column, ascending=False)

            # Apply top-N filter
            if top_n and len(result_df) > top_n:
                result_df = result_df.head(top_n)
                context.log.info(f"Kept top {top_n} results")

            # Reset index
            result_df = result_df.reset_index(drop=True)

            elapsed_time = time.time() - start_time
            context.log.info(f"Reranking completed in {elapsed_time:.2f}s")

            # Calculate costs
            estimated_cost = 0.0
            if track_costs and method == "cohere" and total_api_calls > 0:
                estimated_cost = (total_api_calls / 1000.0) * COHERE_RERANK_COST_PER_1K_SEARCHES

            # Metadata
            metadata = {
                "method": method,
                "model": model or "default",
                "input_documents": len(input_df),
                "output_documents": len(result_df),
                "unique_queries": len(set(queries)),
                "elapsed_time_seconds": f"{elapsed_time:.2f}",
            }

            if method == "cohere":
                metadata["api_calls"] = total_api_calls
            if estimated_cost > 0:
                metadata["estimated_cost_usd"] = f"${estimated_cost:.4f}"
            if rerank_threshold:
                metadata["rerank_threshold"] = rerank_threshold
            if top_n:
                metadata["top_n"] = top_n

            # Add score statistics
            if len(result_df) > 0:
                scores = result_df[output_score_column].tolist()
                metadata["score_mean"] = f"{np.mean(scores):.4f}"
                metadata["score_median"] = f"{np.median(scores):.4f}"
                metadata["score_min"] = f"{np.min(scores):.4f}"
                metadata["score_max"] = f"{np.max(scores):.4f}"

            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(result_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[reranker_asset])
