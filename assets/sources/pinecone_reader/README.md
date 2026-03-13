# Pinecone Reader Component

Query a Pinecone vector index for similar vectors, returning results as a Dagster asset DataFrame.

## Overview

The `PineconeReaderComponent` queries a Pinecone index using similarity search and returns matching records with their scores and metadata. Supports text queries (auto-embedded via OpenAI) or pre-computed embedding vectors.

## Use Cases

- **Semantic search**: Find similar documents or passages
- **RAG pipelines**: Retrieve context for LLM generation
- **Product recommendations**: Find similar products by embedding
- **Content discovery**: Surface related content

## Configuration

### Search by text

```yaml
type: dagster_component_templates.PineconeReaderComponent
attributes:
  asset_name: similar_articles
  index_name: article-embeddings
  query_text: "climate change renewable energy"
  n_results: 20
  include_metadata: true
  group_name: sources
```

### Search with metadata filter

```yaml
type: dagster_component_templates.PineconeReaderComponent
attributes:
  asset_name: filtered_products
  index_name: product-embeddings
  query_text: "wireless headphones"
  n_results: 10
  filter:
    category: electronics
    in_stock: true
```

## Output Schema

| Column | Description |
|--------|-------------|
| `id` | Pinecone record ID |
| `score` | Similarity score (higher = more similar) |
| `[metadata fields]` | Any metadata fields stored in Pinecone |

## Requirements

- `PINECONE_API_KEY` environment variable set
- `OPENAI_API_KEY` if using `query_text`

## Dependencies

- `pandas>=1.5.0`
- `pinecone-client>=3.0.0`
- `openai>=1.0.0` (optional, for text embedding)
