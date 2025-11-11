# Vector Store Writer Asset

Write embeddings to vector databases (Pinecone, ChromaDB, Qdrant, FAISS) with metadata and batch operations. Works seamlessly with Dagster's IO manager pattern - connect embedding assets and automatically store vectors!

## Overview

Store vector embeddings for semantic search and RAG systems. Supports ChromaDB (local), Pinecone (cloud), Qdrant, and FAISS.

**Compatible with:**
- Embedding Generator (with `output_format: dataframe`)
- Any asset producing DataFrame with embedding column
- DataFrame Transformer (operating on embedding DataFrames)

## Features

- **Multiple Providers**: Pinecone, ChromaDB, Qdrant, FAISS
- **Batch Writing**: Efficient batch operations
- **Metadata Support**: Store text and custom metadata with vectors
- **Upsert Support**: Update existing vectors
- **IO Manager Compatible**: Works with upstream embedding assets via visual connections

## Configuration

### Required
- **asset_name** (string), **provider** (string), **collection_name** (string)

### Optional
- **embedding_column** (string) - default: `"embedding"`
- **text_column** (string) - default: `"text"`
- **metadata_columns** (list) - Additional metadata columns
- **connection_string** (string) - Connection string/path
- **api_key** (string) - API key (`${API_KEY}`)
- **batch_size** (integer) - default: `100`
- **upsert** (boolean) - default: `true`

## Input & Output

### Input Requirements
Accepts embeddings DataFrame from upstream assets via IO manager:
- **DataFrame**: Must have an `embedding` column (configurable via `embedding_column` parameter)
- **Optional columns**: `text` column (configurable via `text_column`) and any metadata columns

### Output Format
Returns a dictionary with status information:
- `status`: Success/failure indicator
- `vectors_written`: Number of vectors stored
- `collection_name`: Name of the collection

## Example Pipeline

### Complete RAG Indexing Pipeline

```yaml
# 1. Extract text from documents
- type: DocumentTextExtractorComponent
  attributes:
    asset_name: docs_text
    file_path: /path/to/docs/

# 2. Chunk documents
- type: TextChunkerComponent
  attributes:
    asset_name: doc_chunks
    chunking_strategy: fixed_tokens
    chunk_size: 512
    output_format: dataframe

# 3. Generate embeddings
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: embeddings
    provider: openai
    model: text-embedding-3-small
    api_key: ${OPENAI_API_KEY}
    output_format: dataframe

# 4. Write to vector store
- type: VectorStoreWriterComponent
  attributes:
    asset_name: vector_index
    provider: chromadb
    collection_name: my_docs
    connection_string: ./chroma_db
```

**Visual Connections:**
```
docs_text → doc_chunks → embeddings → vector_index
```

## Standalone Examples

### ChromaDB (Local)
```yaml
type: dagster_component_templates.VectorStoreWriterComponent
attributes:
  asset_name: vector_index
  provider: chromadb
  collection_name: documents
  connection_string: ./chroma_db
```

### Pinecone (Cloud)
```yaml
type: dagster_component_templates.VectorStoreWriterComponent
attributes:
  asset_name: pinecone_index
  provider: pinecone
  collection_name: my-index
  api_key: ${PINECONE_API_KEY}
```

### With Custom Metadata Columns
```yaml
# Upstream: Embedding Generator with DataFrame output
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: doc_embeddings
    provider: openai
    output_format: dataframe

# Write with metadata
- type: VectorStoreWriterComponent
  attributes:
    asset_name: indexed_docs
    provider: chromadb
    collection_name: documents
    embedding_column: embedding
    text_column: text
    metadata_columns: ["title", "author", "date", "category"]
```

## Requirements
- chromadb >= 0.4.0, pinecone-client >= 3.0.0
- qdrant-client >= 1.7.0, faiss-cpu >= 1.7.4
- pandas >= 2.0.0, numpy >= 1.24.0

## License
MIT License
