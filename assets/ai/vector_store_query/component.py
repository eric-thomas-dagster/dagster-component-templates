"""Vector Store Query Asset Component.

Query vector databases for semantic search using embeddings.
"""

import os
from typing import Optional
import pandas as pd

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
)
from pydantic import Field


class VectorStoreQueryComponent(Component, Model, Resolvable):
    """Component for querying vector stores.

    Accepts query embeddings from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.VectorStoreQueryComponent
        attributes:
          asset_name: search_results
          provider: chromadb
          collection_name: documents
          top_k: 10
          api_key: ${PINECONE_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="Provider: 'pinecone', 'weaviate', 'chromadb', 'faiss', 'qdrant'")
    collection_name: str = Field(description="Collection/index name")
    query_text: Optional[str] = Field(default=None, description="Direct query text (alternative to embedding)")
    top_k: int = Field(default=10, description="Number of results")
    connection_string: Optional[str] = Field(default=None, description="Connection string")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    filter_metadata: Optional[str] = Field(default=None, description="JSON filter for metadata")
    include_distances: bool = Field(default=True, description="Include similarity distances")
    embedding_column: str = Field(default="embedding", description="Column name containing query embeddings in the upstream DataFrame")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
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
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with query embeddings")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        collection_name = self.collection_name
        query_text = self.query_text
        top_k = self.top_k
        connection_string = self.connection_string
        api_key = self.api_key
        filter_metadata = self.filter_metadata
        include_distances = self.include_distances
        embedding_column = self.embedding_column
        description = self.description or f"Query {provider}/{collection_name}"
        group_name = self.group_name
        upstream_asset_key = self.upstream_asset_key

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
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def vector_store_query_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Query vector store using embeddings from each row of upstream DataFrame."""

            input_df = upstream

            if embedding_column not in input_df.columns and not query_text:
                raise ValueError(
                    f"Embedding column '{embedding_column}' not found in upstream DataFrame "
                    f"and no query_text provided. Available: {list(input_df.columns)}"
                )

            context.log.info(f"Querying {provider} for top {top_k} results for {len(input_df)} rows")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.warning(f"Environment variable not set: {var_name}")

            all_results = []

            for idx, row in input_df.iterrows():
                query_embedding = None
                if embedding_column in input_df.columns:
                    emb = row[embedding_column]
                    if isinstance(emb, list):
                        query_embedding = emb
                    elif hasattr(emb, 'tolist'):
                        query_embedding = emb.tolist()

                results = []

                if provider == "chromadb":
                    import chromadb
                    client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
                    collection = client.get_collection(name=collection_name)

                    query_results = collection.query(
                        query_embeddings=[query_embedding] if query_embedding else None,
                        query_texts=[query_text] if query_text else None,
                        n_results=top_k
                    )

                    for i in range(len(query_results['ids'][0])):
                        result = {
                            "query_row_index": idx,
                            "id": query_results['ids'][0][i],
                            "document": query_results['documents'][0][i] if 'documents' in query_results else None,
                            "metadata": query_results['metadatas'][0][i] if 'metadatas' in query_results else {},
                        }
                        if include_distances and 'distances' in query_results:
                            result["distance"] = query_results['distances'][0][i]
                        results.append(result)

                elif provider == "pinecone":
                    from pinecone import Pinecone
                    pc = Pinecone(api_key=expanded_api_key)
                    index = pc.Index(collection_name)

                    query_response = index.query(vector=query_embedding, top_k=top_k, include_metadata=True)

                    for match in query_response['matches']:
                        results.append({
                            "query_row_index": idx,
                            "id": match['id'],
                            "score": match['score'],
                            "metadata": match.get('metadata', {})
                        })

                elif provider == "qdrant":
                    from qdrant_client import QdrantClient
                    client = QdrantClient(url=connection_string or "localhost", api_key=expanded_api_key)

                    search_results = client.search(
                        collection_name=collection_name,
                        query_vector=query_embedding,
                        limit=top_k
                    )

                    for hit in search_results:
                        results.append({
                            "query_row_index": idx,
                            "id": hit.id,
                            "score": hit.score,
                            "payload": hit.payload
                        })

                elif provider == "faiss":
                    import faiss
                    import pickle
                    import numpy as np

                    index_path = connection_string or f"./{collection_name}.faiss"
                    metadata_path = index_path.replace(".faiss", "_metadata.pkl")

                    faiss_index = faiss.read_index(index_path)
                    query_vector = np.array([query_embedding]).astype('float32')

                    distances, indices = faiss_index.search(query_vector, top_k)

                    with open(metadata_path, 'rb') as f:
                        metadata = pickle.load(f)

                    for i, doc_idx in enumerate(indices[0]):
                        results.append({
                            "query_row_index": idx,
                            "index": int(doc_idx),
                            "distance": float(distances[0][i]),
                            "metadata": metadata[doc_idx] if doc_idx < len(metadata) else {}
                        })

                else:
                    raise ValueError(f"Unsupported provider: {provider}")

                all_results.extend(results)

            result_df = pd.DataFrame(all_results)

            context.log.info(f"Found {len(result_df)} total results for {len(input_df)} queries")
            context.add_output_metadata({
                "num_queries": len(input_df),
                "num_results": len(result_df),
                "provider": provider,
            })

            return result_df

        return Definitions(assets=[vector_store_query_asset])
