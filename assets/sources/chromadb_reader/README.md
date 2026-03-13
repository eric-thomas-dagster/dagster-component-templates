# ChromaDB Reader Component

Query a ChromaDB collection for similar documents using text queries or pre-computed embeddings, returning results as a Dagster asset DataFrame.

## Overview

The `ChromadbReaderComponent` connects to a local persistent or in-memory ChromaDB instance and performs similarity search. Results are flattened into a DataFrame with one row per result per query.

## Use Cases

- **Document retrieval**: Find similar text chunks for RAG pipelines
- **Semantic search**: Query knowledge bases
- **Content discovery**: Explore related documents
- **Local vector search**: Lightweight alternative to cloud vector DBs

## Configuration

### Persistent ChromaDB

```yaml
type: dagster_component_templates.ChromadbReaderComponent
attributes:
  asset_name: kb_results
  collection_name: knowledge_base
  persist_directory: ./chroma_db
  query_texts:
    - "how to configure authentication"
  n_results: 5
```

### Multiple queries with filter

```yaml
type: dagster_component_templates.ChromadbReaderComponent
attributes:
  asset_name: filtered_docs
  collection_name: documents
  persist_directory: ./chroma_db
  query_texts:
    - "machine learning"
    - "deep learning"
  n_results: 10
  where:
    source: web
  include:
    - documents
    - metadatas
    - distances
```

## Output Schema

| Column | Description |
|--------|-------------|
| `query_idx` | Index of the query that produced this result |
| `id` | ChromaDB document ID |
| `document` | Document text content |
| `distance` | Distance score (lower = more similar) |
| `[metadata fields]` | Any metadata stored with the document |

## Dependencies

- `pandas>=1.5.0`
- `chromadb>=0.4.0`
