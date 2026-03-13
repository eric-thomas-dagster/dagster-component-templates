# PgvectorAssetComponent

Generate text embeddings via the OpenAI Embeddings API and upsert them into a PostgreSQL table using the [`pgvector`](https://github.com/pgvector/pgvector) extension — all as a single observable Dagster asset.

## Use case

In a typical RAG or semantic-search pipeline:

1. A raw or staging table holds document text (product descriptions, support tickets, articles, etc.).
2. `PgvectorAssetComponent` reads that text, calls OpenAI to embed it, and upserts the vectors into a `pgvector`-enabled table.
3. Downstream retrieval assets query the embeddings table using PostgreSQL's `<->` (L2) or `<=>` (cosine) distance operators.

## Prerequisites

```bash
pip install openai pgvector sqlalchemy psycopg2-binary
```

Your PostgreSQL instance must have the `pgvector` extension available. The component runs `CREATE EXTENSION IF NOT EXISTS vector` automatically.

## Example

```yaml
type: dagster_component_templates.PgvectorAssetComponent
attributes:
  asset_name: product_description_embeddings
  database_url_env_var: POSTGRES_URL
  source_table: public.products
  id_column: product_id
  text_column: description
  target_table: product_embeddings
  embedding_model: text-embedding-3-small
  dimensions: 1536
  batch_size: 100
  if_exists: upsert
  group_name: vector_store
  deps:
    - raw/products
```

## Target Table Schema

The component creates (if it does not exist) a table with the following columns:

| Column | Type | Description |
|---|---|---|
| `id` | `TEXT` (PK) | Value of `id_column` from source table |
| `text` | `TEXT` | Raw text that was embedded |
| `embedding` | `vector(N)` | Float vector of dimension `dimensions` |
| `embedded_at` | `TIMESTAMPTZ` | Timestamp of embedding generation |

## `if_exists` Behaviour

| Value | Behaviour |
|---|---|
| `upsert` (default) | `INSERT … ON CONFLICT (id) DO UPDATE SET …` — incremental, safe for partial refreshes |
| `replace` | Drops and recreates the target table before inserting — full refresh |

## Similarity Search

After materialisation, query embeddings directly in SQL:

```sql
SELECT id, text, embedding <=> '[0.1, 0.2, ...]'::vector AS distance
FROM product_embeddings
ORDER BY distance
LIMIT 10;
```

## Fields Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `database_url_env_var` | `str` | required | Env var with PostgreSQL connection URL |
| `source_table` | `str` | required | Source table (`schema.table` or `table`) |
| `id_column` | `str` | required | Primary key column in source table |
| `text_column` | `str` | required | Text column to embed |
| `target_table` | `str` | required | Destination embeddings table |
| `embedding_model` | `str` | `"text-embedding-3-small"` | OpenAI embedding model |
| `openai_api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var with OpenAI API key |
| `dimensions` | `int` | `1536` | Vector dimensionality |
| `batch_size` | `int` | `100` | Texts per OpenAI API request |
| `if_exists` | `str` | `"upsert"` | `"upsert"` or `"replace"` |
| `group_name` | `str` | `"vector_store"` | Dagster asset group |
| `deps` | `list[str]` | `None` | Upstream asset keys |
