# pgvector Reader Component

Query a PostgreSQL table with the pgvector extension for similar vectors, returning results as a Dagster asset DataFrame.

## Overview

The `PgvectorReaderComponent` connects to a PostgreSQL database and performs approximate nearest-neighbor search using pgvector operators. It can embed query text via OpenAI or accept a pre-computed embedding vector.

## Use Cases

- **Semantic search**: Find similar documents by text query
- **Recommendation**: Retrieve similar items by embedding
- **RAG pipelines**: Retrieve context chunks for LLM generation
- **Duplicate detection**: Find near-duplicate records

## Distance Metrics

| Metric | Operator | Best For |
|--------|----------|---------|
| `cosine` | `<=>` | Normalized embeddings (most common) |
| `l2` | `<->` | Euclidean distance |
| `inner_product` | `<#>` | Pre-normalized vectors |

## Configuration

### Search by text (auto-embed)

```yaml
type: dagster_component_templates.PgvectorReaderComponent
attributes:
  asset_name: similar_docs
  table_name: document_embeddings
  query_text: "machine learning fundamentals"
  n_results: 20
  distance_metric: cosine
```

### Search by pre-computed embedding

```yaml
type: dagster_component_templates.PgvectorReaderComponent
attributes:
  asset_name: similar_products
  table_name: product_embeddings
  query_embedding: [0.1, 0.2, ...]
  n_results: 10
```

## Output Schema

| Column | Description |
|--------|-------------|
| `[all table columns]` | Original table columns |
| `distance` | Distance score (lower = more similar for cosine/l2) |

## Requirements

- PostgreSQL with pgvector extension installed
- `DATABASE_URL` environment variable set
- `OPENAI_API_KEY` if using `query_text`

## Dependencies

- `pandas>=1.5.0`
- `sqlalchemy>=2.0.0`
- `psycopg2-binary>=2.9.0`
- `pgvector>=0.2.0`
- `openai>=1.0.0` (optional, for text embedding)
