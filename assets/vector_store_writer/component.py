"""Vector Store Writer Asset Component.

Write embeddings to vector databases (Pinecone, Weaviate, Chroma, FAISS, Qdrant).
Supports batch writing and metadata attachment.
"""

import os
from typing import Optional, List, Dict, Any
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
    upstream_asset_keys: Optional[str] = Field(
        default=None,
        description='Comma-separated list of upstream asset keys to load DataFrames from (automatically set by custom lineage)'
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
        upstream_asset_keys_str = self.upstream_asset_keys

        # Parse upstream asset keys if provided
        upstream_keys = []
        if upstream_asset_keys_str:
            upstream_keys = [k.strip() for k in upstream_asset_keys_str.split(',')]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def vector_store_writer_asset(context: AssetExecutionContext, **kwargs) -> Dict[str, Any]:
            """Write embeddings to vector store.

            Accepts embeddings from upstream assets via IO manager.
            Compatible with: Embedding Generator or any DataFrame with embeddings.
            """

            # Load upstream assets based on configuration
            upstream_assets = {}

            # If upstream_asset_keys is configured, try to load assets explicitly
            if upstream_keys and hasattr(context, 'load_asset_value'):
                # Real execution context - load assets explicitly
                context.log.info(f"Loading {len(upstream_keys)} upstream asset(s) via context.load_asset_value()")
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(key)
                        upstream_assets[key] = value
                        context.log.info(f"  - Loaded '{key}': {type(value).__name__}")
                    except Exception as e:
                        context.log.error(f"  - Failed to load '{key}': {e}")
                        raise
            else:
                # Preview/mock context or no upstream_keys - fall back to kwargs
                upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"Vector Store Writer '{asset_name}' requires at least one upstream asset "
                    "that produces embeddings. Connect an asset like Embedding Generator."
                )

            context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Find DataFrame with embeddings
            df = None
            for key, value in upstream_assets.items():
                if isinstance(value, pd.DataFrame):
                    if embedding_column in value.columns:
                        df = value
                        context.log.info(f"Using embeddings from '{key}' ({len(value)} rows)")
                        break
                    else:
                        context.log.warning(f"DataFrame from '{key}' missing column '{embedding_column}'")

            if df is None:
                raise ValueError(
                    f"No DataFrame with '{embedding_column}' column found in upstream assets. "
                    f"Received: {list(upstream_assets.keys())}"
                )

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
