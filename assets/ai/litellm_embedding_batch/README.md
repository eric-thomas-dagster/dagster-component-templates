# LiteLLM Embedding Batch

Generate embeddings for a text column using LiteLLM with automatic model routing and fallback.

## Overview

`LitellmEmbeddingBatchComponent` reads an upstream Dagster asset as a DataFrame, generates embedding vectors using `litellm.embedding()` in configurable batches, and writes the resulting vectors (lists of floats) to a new column.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `output_column` | string | `"embedding"` | Column to write embedding vectors |
| `model` | string | `"text-embedding-3-small"` | Embedding model |
| `batch_size` | integer | `100` | Rows per API call |
| `dimensions` | integer | null | Embedding dimensions (for models that support it) |
| `fallback_models` | list | null | Models to try if primary fails |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmEmbeddingBatchComponent
attributes:
  asset_name: article_embeddings
  upstream_asset_key: processed_articles
  text_column: body
  model: text-embedding-3-small
  batch_size: 100
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
