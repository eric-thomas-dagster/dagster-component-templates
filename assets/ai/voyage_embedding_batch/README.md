# Voyage Embedding Batch

> **🔑 API key required.** Set `VOYAGE_API_KEY` in your environment. Get a key at [https://dash.voyageai.com/api-keys](https://dash.voyageai.com/api-keys).

Generate embeddings for a text column using Voyage AI's native SDK, in configurable batches. Voyage's `voyage-3-large` currently tops the MTEB retrieval benchmark — reach for this component when retrieval quality is the main lever for your RAG system.

## When to reach for this vs the LiteLLM path

| | `voyage_embedding_batch` | `litellm_embedding_batch` |
|---|---|---|
| Provider | Voyage only | OpenAI / Cohere / Voyage / any LiteLLM-supported |
| Dependency | `voyageai` (~2 MB) | `litellm` (~40 MB + provider deps) |
| Retrieval quality (English MTEB) | Best-in-class | Depends on model |
| Asymmetric embeddings (`input_type: query \| document`) | ✅ First-class field | Requires manual wire-up |
| Domain-tuned models (`voyage-code-3` / `voyage-finance-2` / `voyage-law-2`) | ✅ | Model string only |
| Fallback across providers | ❌ | ✅ |

If you're single-vendor + care about retrieval quality → this component. If you want cross-provider fallback or already use LiteLLM → the LiteLLM sibling.

## Quick example

```yaml
type: dagster_community_components.VoyageEmbeddingBatchComponent
attributes:
  asset_name: article_embeddings
  upstream_asset_key: chunked_articles
  text_column: chunk_text
  model: voyage-3-large        # SOTA MTEB retrieval
  input_type: document         # asymmetric embeddings — use 'query' at search time
  output_dimension: 1024       # 256 / 512 / 1024 (default) / 2048
  batch_size: 128
  api_key_env_var: VOYAGE_API_KEY
```

## Asymmetric embeddings (`input_type`)

Voyage models produce **different** embedding vectors for the same text depending on whether it's being indexed (`input_type: document`) or used as a search query (`input_type: query`). Voyage's benchmarks show a 4–6 point NDCG@10 lift when this asymmetry is set correctly.

**Rule of thumb:**
- Corpus indexing → `input_type: document`
- Runtime search query → `input_type: query`
- Don't know / don't care → omit (symmetric mode, works but lower quality)

## Model shortlist

| Model | When to use | Dimensions |
|---|---|---|
| `voyage-3-large` | Default. Best English retrieval. | 256 / 512 / 1024 / 2048 |
| `voyage-3` | Cheaper English retrieval; strong quality. | 1024 |
| `voyage-3-lite` | Bulk-embedding budget option. | 512 |
| `voyage-code-3` | Code retrieval; trained on GitHub-scale corpus. | 256 / 512 / 1024 / 2048 |
| `voyage-multilingual-2` | 100+ languages, retrieval-tuned. | 1024 |
| `voyage-finance-2` | Financial docs, filings, earnings calls. | 1024 |
| `voyage-law-2` | Legal contracts, case law. | 1024 |

See [Voyage's docs](https://docs.voyageai.com/docs/embeddings) for the full list + benchmarks.

## Metadata emitted per materialization

- `voyage_model` — model used
- `embedding_dim` — vector dimensionality
- `total_tokens` — sum across batches (for cost tracking; promote to Dagster+ Insights)
- `input_type` — asymmetric mode
- `dagster/row_count`, `dagster/column_schema`, `dagster/column_lineage`

Cost tracking is opt-in: promote `total_tokens` to a Dagster+ Insights custom metric via the UI, then set alerts on daily embedding budgets.

## Partitioning

Same partition-field shape as sibling embedding components — supports `daily` / `weekly` / `monthly` / `hourly` / `static` / `multi` / `dynamic` partitioning, with `partition_date_column` / `partition_static_column` for upstream filtering. See the schema for the full field list.
