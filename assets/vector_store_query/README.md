# Vector Store Query Asset

Query vector databases for semantic search using embeddings. Returns most relevant documents based on similarity. Works seamlessly with Dagster's IO manager pattern - connect query embedding assets and automatically search!

## Overview

Perform semantic search across your vector database. Perfect for RAG systems, document search, and similarity finding.

**Compatible with:**
- Embedding Generator (producing query embeddings as array or list)
- Any asset producing embedding array/list
- Can also accept direct query text (will generate embeddings internally)

## Features

- **Multiple Providers**: ChromaDB, Pinecone, Qdrant, FAISS
- **Configurable Results**: Control number of results (top_k)
- **Metadata Filtering**: Filter results by metadata
- **Distance/Similarity Scores**: Include similarity metrics
- **DataFrame Output**: Results in pandas DataFrame
- **IO Manager Compatible**: Works with upstream embedding assets via visual connections

## Configuration

### Required
- **asset_name** (string), **provider** (string), **collection_name** (string)

### Optional
- **query_text** (string) - Query text (alternative to embedding)
- **top_k** (integer) - Number of results (default: `10`)
- **connection_string** (string), **api_key** (string) - `${API_KEY}`
- **include_distances** (boolean) - default: `true`

## Input & Output

### Input Requirements
Accepts query embeddings from upstream assets via IO manager:
- **Array**: NumPy array of query embedding
- **List**: Python list of query embedding values
- **Alternative**: Can use `query_text` parameter instead of upstream input (will generate embeddings internally)

### Output Format
Returns a Pandas DataFrame with search results containing:
- `id`: Document identifier
- `score`: Similarity score
- `text`: Document text content
- `metadata`: Additional metadata columns (if available)

## Example Pipeline

### Query Embedding → Vector Search

```yaml
# 1. Generate query embedding
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: query_embedding
    provider: openai
    model: text-embedding-3-small
    text_input: "How do I deploy to production?"
    api_key: ${OPENAI_API_KEY}
    output_format: array

# 2. Search vector store
- type: VectorStoreQueryComponent
  attributes:
    asset_name: search_results
    provider: chromadb
    collection_name: my_docs
    connection_string: ./chroma_db
    top_k: 5
```

**Visual Connections:**
```
query_embedding → search_results
```

## Standalone Examples

### Basic Query with Direct Text
```yaml
type: dagster_component_templates.VectorStoreQueryComponent
attributes:
  asset_name: search_results
  provider: chromadb
  collection_name: documents
  query_text: "How do I deploy to production?"
  top_k: 5
```

### With Pinecone
```yaml
type: dagster_component_templates.VectorStoreQueryComponent
attributes:
  asset_name: pinecone_results
  provider: pinecone
  collection_name: my-index
  query_text: "${USER_QUERY}"
  api_key: ${PINECONE_API_KEY}
  top_k: 10
```

### From Upstream Embedding
```yaml
# Generate embedding for query
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: user_query_embedding
    provider: openai
    model: text-embedding-3-small
    text_input: "${USER_QUERY}"
    api_key: ${OPENAI_API_KEY}
    output_format: list

# Search with embedding
- type: VectorStoreQueryComponent
  attributes:
    asset_name: relevant_docs
    provider: chromadb
    collection_name: documents
    top_k: 10
    include_distances: true
```

## Requirements
- chromadb >= 0.4.0, pinecone-client >= 3.0.0
- qdrant-client >= 1.7.0, faiss-cpu >= 1.7.4
- pandas >= 2.0.0

## License
MIT License
