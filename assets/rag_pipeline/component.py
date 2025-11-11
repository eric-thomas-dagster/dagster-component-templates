"""RAG Pipeline Asset Component.

Complete Retrieval-Augmented Generation pipeline combining query, retrieval, and generation.
"""

import os
from typing import Optional

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


class RAGPipelineComponent(Component, Model, Resolvable):
    """Component for RAG pipeline.

    Accepts query from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.RAGPipelineComponent
        attributes:
          asset_name: rag_response
          vector_store_provider: chromadb
          collection_name: documents
          llm_provider: openai
          llm_model: gpt-4
          llm_api_key: ${OPENAI_API_KEY}
          embedding_api_key: ${OPENAI_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    vector_store_provider: str = Field(description="Vector store provider")
    collection_name: str = Field(description="Collection name")
    llm_provider: str = Field(description="LLM provider")
    llm_model: str = Field(description="LLM model")
    embedding_provider: str = Field(default="openai", description="Embedding provider")
    embedding_model: str = Field(default="text-embedding-3-small", description="Embedding model")
    top_k: int = Field(default=5, description="Number of documents to retrieve")
    vector_store_connection: Optional[str] = Field(default=None, description="Vector store connection")
    llm_api_key: Optional[str] = Field(default=None, description="LLM API key with ${VAR_NAME} syntax")
    embedding_api_key: Optional[str] = Field(default=None, description="Embedding API key with ${VAR_NAME} syntax")
    include_sources: bool = Field(default=True, description="Include source documents")
    temperature: float = Field(default=0.7, description="LLM temperature")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        vector_store_provider = self.vector_store_provider
        collection_name = self.collection_name
        llm_provider = self.llm_provider
        llm_model = self.llm_model
        embedding_provider = self.embedding_provider
        embedding_model = self.embedding_model
        top_k = self.top_k
        vector_store_connection = self.vector_store_connection
        llm_api_key = self.llm_api_key
        embedding_api_key = self.embedding_api_key
        include_sources = self.include_sources
        temperature = self.temperature
        description = self.description or "RAG pipeline"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def rag_pipeline_asset(ctx: AssetExecutionContext, **kwargs):
            """Execute RAG pipeline.

            Accepts query from upstream assets via IO manager.
            Compatible with: Any asset producing query text (str, dict with 'query' key, etc.)

            This component orchestrates a complete RAG pipeline:
            1. Generate query embeddings
            2. Retrieve relevant documents from vector store
            3. Generate response using LLM with retrieved context
            """

            # Get query from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"RAG Pipeline '{asset_name}' requires at least one upstream asset "
                    "that produces a query. Connect a query-producing asset."
                )

            ctx.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Extract query from first upstream asset
            query = None
            for key, value in upstream_assets.items():
                if isinstance(value, str):
                    query = value
                    ctx.log.info(f"Using query from '{key}' (string)")
                    break
                elif isinstance(value, dict):
                    if 'query' in value:
                        query = value['query']
                        ctx.log.info(f"Using query from '{key}' dict['query']")
                        break
                    elif 'text' in value:
                        query = value['text']
                        ctx.log.info(f"Using query from '{key}' dict['text']")
                        break

            if not query:
                raise ValueError(
                    f"No query found in upstream assets. Received: {list(upstream_assets.keys())}. "
                    f"Ensure upstream assets produce query as string or dict with 'query' or 'text' key."
                )

            # Expand environment variables in API keys
            expanded_llm_api_key = None
            if llm_api_key:
                expanded_llm_api_key = os.path.expandvars(llm_api_key)
                if expanded_llm_api_key == llm_api_key and '${' in llm_api_key:
                    raise ValueError(f"Environment variable in llm_api_key '{llm_api_key}' is not set")

            expanded_embedding_api_key = None
            if embedding_api_key:
                expanded_embedding_api_key = os.path.expandvars(embedding_api_key)
                if expanded_embedding_api_key == embedding_api_key and '${' in embedding_api_key:
                    raise ValueError(f"Environment variable in embedding_api_key '{embedding_api_key}' is not set")

            ctx.log.info(f"RAG query: {query}")

            # Step 1: Generate query embedding
            ctx.log.info(f"Generating query embedding with {embedding_provider}")

            query_embedding = None

            if embedding_provider == "openai":
                import openai
                client = openai.OpenAI(api_key=expanded_embedding_api_key)
                response = client.embeddings.create(model=embedding_model, input=[query])
                query_embedding = response.data[0].embedding
            elif embedding_provider == "sentence_transformers":
                from sentence_transformers import SentenceTransformer
                model = SentenceTransformer(embedding_model)
                query_embedding = model.encode([query])[0].tolist()
            else:
                raise ValueError(f"Unsupported embedding provider: {embedding_provider}")

            # Step 2: Query vector store
            ctx.log.info(f"Querying vector store for top {top_k} documents")

            retrieved_docs = []

            if vector_store_provider == "chromadb":
                import chromadb
                client = chromadb.PersistentClient(path=vector_store_connection or "./chroma_db")
                collection = client.get_collection(name=collection_name)

                results = collection.query(query_embeddings=[query_embedding], n_results=top_k)

                for i in range(len(results['ids'][0])):
                    retrieved_docs.append({
                        "text": results['documents'][0][i] if 'documents' in results else "",
                        "metadata": results['metadatas'][0][i] if 'metadatas' in results else {}
                    })
            elif vector_store_provider == "pinecone":
                from pinecone import Pinecone
                pc = Pinecone(api_key=expanded_embedding_api_key)
                index = pc.Index(collection_name)

                results = index.query(vector=query_embedding, top_k=top_k, include_metadata=True)

                for match in results['matches']:
                    retrieved_docs.append({
                        "text": match.get('metadata', {}).get('text', ''),
                        "metadata": match.get('metadata', {})
                    })
            else:
                raise ValueError(f"Unsupported vector store provider: {vector_store_provider}")

            ctx.log.info(f"Retrieved {len(retrieved_docs)} documents")

            # Step 3: Build context
            context_text = "\n\n".join([doc["text"] for doc in retrieved_docs])

            # Step 4: Generate response with LLM
            ctx.log.info(f"Generating response with {llm_provider}/{llm_model}")

            prompt = f"""Answer the following question based on the provided context.

Context:
{context_text}

Question: {query}

Answer:"""

            if llm_provider == "openai":
                import openai
                client = openai.OpenAI(api_key=expanded_llm_api_key)
                response = client.chat.completions.create(
                    model=llm_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature
                )
                answer = response.choices[0].message.content
            elif llm_provider == "anthropic":
                import anthropic
                client = anthropic.Anthropic(api_key=expanded_llm_api_key)
                message = client.messages.create(
                    model=llm_model,
                    max_tokens=4096,
                    temperature=temperature,
                    messages=[{"role": "user", "content": prompt}]
                )
                answer = message.content[0].text
            else:
                raise ValueError(f"Unsupported LLM provider: {llm_provider}")

            ctx.log.info("RAG pipeline completed")

            result = {
                "query": query,
                "answer": answer,
                "num_sources": len(retrieved_docs)
            }

            if include_sources:
                result["sources"] = retrieved_docs

            ctx.add_output_metadata({
                "query": query,
                "num_sources": len(retrieved_docs),
                "answer_length": len(answer)
            })

            return result

        return Definitions(assets=[rag_pipeline_asset])
