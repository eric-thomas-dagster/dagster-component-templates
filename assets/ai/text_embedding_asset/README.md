# text_embedding_asset

Embed literal text strings from your defs.yaml config into a Dagster asset (pandas DataFrame). One row per text. Uses OpenAI's `text-embedding-3-*` API or any OpenAI-compatible endpoint (Azure OpenAI, Vercel AI Gateway, LiteLLM, etc.).

## When to use this vs `embeddings_generator`

- **`text_embedding_asset`** (this): "I have literal strings in config" — query embeddings for RAG, intent-label pre-embedding, one-shot bookmark text.
- **`embeddings_generator`**: "I have an upstream DataFrame with a text column, embed each row." Row-wise pattern.

## Example — RAG query embedding

```yaml
type: dagster_community_components.TextEmbeddingAssetComponent
attributes:
  asset_name: rag_query_embedding
  texts:
    - "How does Dagster orchestrate around long-running Temporal workflows?"
  model: text-embedding-3-small
  api_key_env_var: OPENAI_API_KEY
  dimensions: 1536
  group_name: rag
```

Feeds directly into `SupabaseVectorSearchAssetComponent` (or any pgvector consumer) via `upstream_asset_key: rag_query_embedding` + `query_embedding_column: embedding`.

## Example — Vercel AI Gateway routing

```yaml
attributes:
  # ...
  api_key_env_var: VERCEL_AI_TOKEN
  api_base_env_var: VERCEL_AI_BASE  # e.g. https://ai-gateway.vercel.sh/v1
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_name` | string | Output asset name. |
| `texts` | list[str] | Literal strings to embed. |
| `model` | string | Embedding model. Default `text-embedding-3-small`. |
| `api_key_env_var` | string | Default `OPENAI_API_KEY`. |
| `api_base_env_var` | string | Custom base URL env var for OpenAI-compatible endpoints. |
| `dimensions` | int | Only for `text-embedding-3-*` models. |
| `text_column` / `embedding_column` | string | Output column names. Defaults `text` / `embedding`. |

## Output

DataFrame:

```
   text                                              embedding
0  How does Dagster orchestrate around...  [0.024, -0.011, 0.038, ...]  (1536 floats)
```

Metadata surfaces text_count, model, dimensions, total tokens used, preview.

## Requirements

```
openai>=1.0.0
pandas>=1.5.0
tabulate>=0.9.0
```
