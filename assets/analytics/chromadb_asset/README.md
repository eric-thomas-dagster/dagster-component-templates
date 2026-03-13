# ChromadbAssetComponent

Read documents from a relational database, generate embeddings via the OpenAI Embeddings API, and upsert them into a [ChromaDB](https://www.trychroma.com) collection — all as an observable Dagster asset.

## Use case

In a RAG or semantic-search pipeline:

1. Source text lives in a relational database (PostgreSQL, MySQL, SQLite, etc.).
2. `ChromadbAssetComponent` reads that text, calls OpenAI to embed it, and upserts documents into a ChromaDB collection.
3. Downstream retrieval assets or applications query the collection for nearest-neighbour similarity.

## Prerequisites

```bash
pip install chromadb openai sqlalchemy
```

ChromaDB must be running and reachable (or use Chroma Cloud with `chroma_api_key_env_var`).

## Example

```yaml
type: dagster_component_templates.ChromadbAssetComponent
attributes:
  asset_name: product_chroma_collection
  chroma_host: localhost
  chroma_port: 8000
  collection_name: products
  database_url_env_var: DATABASE_URL
  source_table: public.products
  id_column: product_id
  text_column: description
  metadata_columns:
    - name
    - category
    - price
  embedding_model: text-embedding-3-small
  openai_api_key_env_var: OPENAI_API_KEY
  batch_size: 100
  group_name: vector_store
  deps:
    - raw/products
```

## Chroma Cloud

For Chroma Cloud, provide the cloud endpoint as `chroma_host` and set `chroma_api_key_env_var`:

```yaml
chroma_host: api.trychroma.com
chroma_port: 443
chroma_api_key_env_var: CHROMA_API_KEY
```

## Metadata Filtering

Storing extra columns as metadata enables filtered similarity search:

```yaml
metadata_columns:
  - category
  - brand
  - price_tier
```

In your retrieval application:

```python
collection.query(
    query_embeddings=[my_vector],
    n_results=10,
    where={"category": "electronics"},
)
```

Metadata values are cast to `str` if they are not a native ChromaDB type (`str`, `int`, `float`, `bool`).

## Fields Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `chroma_host` | `str` | `"localhost"` | ChromaDB server host |
| `chroma_port` | `int` | `8000` | ChromaDB server port |
| `chroma_api_key_env_var` | `str` | `None` | Env var with Chroma Cloud API key |
| `collection_name` | `str` | required | ChromaDB collection name |
| `database_url_env_var` | `str` | required | Env var with source DB SQLAlchemy URL |
| `source_table` | `str` | required | Source table (`schema.table` or `table`) |
| `id_column` | `str` | required | Primary key column (used as ChromaDB document ID) |
| `text_column` | `str` | required | Text column to embed |
| `metadata_columns` | `list[str]` | `None` | Extra columns to store as document metadata |
| `embedding_model` | `str` | `"text-embedding-3-small"` | OpenAI embedding model |
| `openai_api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var with OpenAI API key |
| `batch_size` | `int` | `100` | Documents per OpenAI API request and ChromaDB upsert batch |
| `group_name` | `str` | `"vector_store"` | Dagster asset group |
| `deps` | `list[str]` | `None` | Upstream asset keys for lineage |
