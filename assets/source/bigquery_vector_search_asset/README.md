# BigQuery Vector Search Asset

k-NN similarity search against an `ARRAY<FLOAT64>` column in BigQuery using BQ's native [`VECTOR_SEARCH`](https://cloud.google.com/bigquery/docs/vector-search-intro) function. Returns top-K matches per query embedding plus their distances.

```yaml
type: dagster_component_templates.BigqueryVectorSearchAssetComponent
attributes:
  asset_name: top_matches
  base_table: my-project.rag.documents
  base_column: embedding
  select_columns: [doc_id, title, content]
  top_k: 5
  distance_type: COSINE
  upstream_asset_key: query_embeddings
  query_vector_column: vector
  query_id_column: query_id
```

## Two modes

**Static** — inline query vector(s) in YAML, run once:
```yaml
query_vectors:
  - [0.012, -0.034, ...]   # 768-dim vector
```

**From upstream** — one BQ search per row of an upstream DataFrame (typical RAG pattern with `vertex_ai_text_embeddings_asset` upstream):
```yaml
upstream_asset_key: query_embeddings
query_vector_column: vector
query_id_column: query_id
```

## Output shape

One row per (query_id × match), with these columns:
- `query_id` — `q0`, `q1`, ... or values from `query_id_column`
- All columns named in `select_columns` (from the base table)
- `distance` — similarity score (lower = more similar for COSINE/EUCLIDEAN; higher = more similar for DOT_PRODUCT)

## Distance types

| Type | Use for |
|---|---|
| `COSINE` | Normalized embeddings (OpenAI, Vertex AI text-embedding-004, most general purpose) |
| `EUCLIDEAN` | Spatial embeddings, when magnitude matters |
| `DOT_PRODUCT` | Recommended for un-normalized embeddings or when speed > precision |

## Performance — create a vector index

```sql
CREATE VECTOR INDEX my_index
ON `project.dataset.documents`(embedding)
OPTIONS(index_type='IVF', distance_type='COSINE')
```

Without an index, BQ falls back to a full scan — fine for thousands of rows, very slow at millions. Tune ANN recall with `fraction_lists_to_search` (0–1, higher = more accurate).

## Auth

Service account needs `roles/bigquery.dataViewer` on the base table and `roles/bigquery.jobUser` (project-level) to run queries.

## Cost

`VECTOR_SEARCH` is billed as a regular query at $5–6.25/TB scanned. With an index, only a small fraction of the table is scanned per query. Pair with `bigquery_dry_run_check` to enforce cost ceilings on recurring searches.

## Sister components

- `vertex_ai_text_embeddings_asset` — produce embeddings upstream (Vertex AI's text-embedding-004, 768-dim)
- `litellm_embedding_batch` — multi-provider embedding for upstream
- `bigquery_query_asset` — generic BQ SQL (you'd write VECTOR_SEARCH by hand)
- `chroma_resource` / `qdrant_resource` / `weaviate_resource` — non-BQ vector stores
