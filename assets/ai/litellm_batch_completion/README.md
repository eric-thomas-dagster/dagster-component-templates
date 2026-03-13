# LiteLLM Batch Completion

Process a DataFrame column row-by-row through any LLM using LiteLLM. Supports routing, fallbacks, cost tracking, and parallel thread execution.

## Overview

`LitellmBatchCompletionComponent` reads an upstream Dagster asset as a DataFrame, sends each row through a configured LLM via LiteLLM, and returns the enriched DataFrame with responses appended as a new column.

LiteLLM supports 100+ providers: OpenAI, Anthropic, Azure OpenAI, AWS Bedrock, Google Gemini, Mistral, Cohere, Ollama, and more — all through a unified API.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `output_column` | string | `"llm_response"` | Column to write LLM responses to |
| `model` | string | `"gpt-4o-mini"` | LiteLLM model string |
| `system_prompt` | string | null | System message prepended to each request |
| `prompt_template` | string | null | Template using row values e.g. `"Summarize: {text}"` |
| `max_tokens` | integer | `500` | Maximum tokens per completion |
| `temperature` | number | `0.0` | Sampling temperature |
| `fallback_models` | list | null | Models to try if primary fails |
| `max_workers` | integer | `4` | Parallel threads for batch processing |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmBatchCompletionComponent
attributes:
  asset_name: classified_support_tickets
  upstream_asset_key: raw_support_tickets
  text_column: body
  output_column: sentiment_label
  model: gpt-4o-mini
  system_prompt: "Classify the sentiment as positive, neutral, or negative."
  max_tokens: 50
  max_workers: 8
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
