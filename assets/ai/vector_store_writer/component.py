"""Vector Store Writer Asset Component.

Write embeddings to vector databases (Pinecone, Weaviate, Chroma, FAISS, Qdrant).
Supports batch writing and metadata attachment.
"""

import os
from typing import Optional, List, Dict, Any
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
)
from pydantic import Field


class VectorStoreWriterComponent(Component, Model, Resolvable):
    """Component for writing embeddings to vector stores.

    Accepts embeddings from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.VectorStoreWriterComponent
        attributes:
          asset_name: vector_store_index
          provider: chromadb
          collection_name: documents
          api_key: ${PINECONE_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="Provider: 'pinecone', 'weaviate', 'chromadb', 'faiss', 'qdrant'")
    collection_name: str = Field(description="Collection/index name")
    embedding_column: str = Field(default="embedding", description="Column with embeddings")
    text_column: str = Field(default="text", description="Column with text")
    metadata_columns: Optional[List[str]] = Field(default=None, description="Additional metadata columns")
    connection_string: Optional[str] = Field(default=None, description="Connection string or path")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    batch_size: int = Field(default=100, description="Batch size")
    upsert: bool = Field(default=True, description="Upsert if exists")
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
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with embeddings to write"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        collection_name = self.collection_name
        embedding_column = self.embedding_column
        text_column = self.text_column
        metadata_columns = self.metadata_columns or []
        connection_string = self.connection_string
        api_key = self.api_key
        batch_size = self.batch_size
        upsert = self.upsert
        description = self.description or f"Write embeddings to {provider}/{collection_name}"
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
        def vector_store_writer_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Dict[str, Any]:
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
            """Write embeddings to vector store from upstream DataFrame."""

            df = upstream

            if embedding_column not in df.columns:
                raise ValueError(
                    f"Embedding column '{embedding_column}' not found in upstream DataFrame. "
                    f"Available: {list(df.columns)}"
                )

            context.log.info(f"Received DataFrame with {len(df)} rows")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.warning(f"Environment variable not set: {var_name}")
                    # Don't fail - some providers don't need API keys

            context.log.info(f"Writing {len(df)} embeddings to {provider}")

            if provider == "chromadb":
                import chromadb
                client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
                collection = client.get_or_create_collection(name=collection_name)

                embeddings = df[embedding_column].tolist()
                texts = df[text_column].tolist() if text_column in df.columns else None
                ids = [f"doc_{i}" for i in range(len(df))]
                metadatas = df[metadata_columns].to_dict('records') if metadata_columns else None

                for i in range(0, len(df), batch_size):
                    batch_end = min(i + batch_size, len(df))
                    collection.upsert(
                        embeddings=embeddings[i:batch_end],
                        documents=texts[i:batch_end] if texts else None,
                        ids=ids[i:batch_end],
                        metadatas=metadatas[i:batch_end] if metadatas else None
                    )

            elif provider == "pinecone":
                from pinecone import Pinecone
                pc = Pinecone(api_key=expanded_api_key)
                index = pc.Index(collection_name)

                vectors = []
                for idx, row in df.iterrows():
                    metadata = {text_column: row[text_column]} if text_column in df.columns else {}
                    for col in metadata_columns:
                        if col in df.columns:
                            metadata[col] = row[col]
                    vectors.append({
                        "id": f"doc_{idx}",
                        "values": row[embedding_column],
                        "metadata": metadata
                    })

                for i in range(0, len(vectors), batch_size):
                    index.upsert(vectors=vectors[i:i + batch_size])

            elif provider == "qdrant":
                from qdrant_client import QdrantClient
                from qdrant_client.models import Distance, VectorParams, PointStruct

                client = QdrantClient(url=connection_string or "localhost", api_key=expanded_api_key)

                try:
                    client.get_collection(collection_name)
                except:
                    embedding_dim = len(df[embedding_column].iloc[0])
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=embedding_dim, distance=Distance.COSINE)
                    )

                points = []
                for idx, row in df.iterrows():
                    payload = {text_column: row[text_column]} if text_column in df.columns else {}
                    for col in metadata_columns:
                        if col in df.columns:
                            payload[col] = row[col]
                    points.append(PointStruct(id=idx, vector=row[embedding_column], payload=payload))

                for i in range(0, len(points), batch_size):
                    client.upsert(collection_name=collection_name, points=points[i:i + batch_size])

            elif provider == "faiss":
                import faiss
                import pickle

                embeddings = np.array(df[embedding_column].tolist()).astype('float32')
                dimension = embeddings.shape[1]

                # Create or load index
                index_path = connection_string or f"./{collection_name}.faiss"
                metadata_path = index_path.replace(".faiss", "_metadata.pkl")

                if os.path.exists(index_path) and not upsert:
                    index = faiss.read_index(index_path)
                else:
                    index = faiss.IndexFlatL2(dimension)

                index.add(embeddings)

                # Save metadata
                metadata = df[[text_column] + metadata_columns].to_dict('records') if text_column in df.columns else []
                with open(metadata_path, 'wb') as f:
                    pickle.dump(metadata, f)

                faiss.write_index(index, index_path)

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Successfully wrote {len(df)} vectors")
            context.add_output_metadata({"num_vectors": len(df), "provider": provider, "collection": collection_name})

            return {"status": "success", "num_vectors": len(df)}

        return Definitions(assets=[vector_store_writer_asset])
