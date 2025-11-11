"""Vector Store Query Asset Component.

Query vector databases for semantic search using embeddings.
"""

import os
from typing import Optional
import pandas as pd

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
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

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
        description = self.description or f"Query {provider}/{collection_name}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def vector_store_query_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Query vector store.

            Accepts query embeddings from upstream assets via IO manager.
            Can also use query_text for text-based queries (some providers support this).
            """

            # Get query embedding from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}
            query_embedding = None

            if upstream_assets:
                context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

                for key, value in upstream_assets.items():
                    if isinstance(value, list):
                        query_embedding = value[0] if value else None
                        context.log.info(f"Using embedding from '{key}' (list)")
                        break
                    elif hasattr(value, 'tolist'):
                        query_embedding = value.tolist()
                        context.log.info(f"Using embedding from '{key}' (array)")
                        break
                    elif isinstance(value, dict) and 'embedding' in value:
                        query_embedding = value['embedding']
                        context.log.info(f"Using embedding from '{key}' dict")
                        break

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.warning(f"Environment variable not set: {var_name}")

            context.log.info(f"Querying {provider} for top {top_k} results")

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
                    result = {
                        "id": match['id'],
                        "score": match['score'],
                        "metadata": match.get('metadata', {})
                    }
                    results.append(result)

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

                index = faiss.read_index(index_path)
                query_vector = np.array([query_embedding]).astype('float32')

                distances, indices = index.search(query_vector, top_k)

                with open(metadata_path, 'rb') as f:
                    metadata = pickle.load(f)

                for i, idx in enumerate(indices[0]):
                    results.append({
                        "index": int(idx),
                        "distance": float(distances[0][i]),
                        "metadata": metadata[idx] if idx < len(metadata) else {}
                    })

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Found {len(results)} results")
            context.add_output_metadata({"num_results": len(results)})

            return pd.DataFrame(results)

        return Definitions(assets=[vector_store_query_asset])
