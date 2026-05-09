# Gemini LLM (text)

Native single-vendor text LLM component for Google's Gemini text models — `gemini-2.5-flash`, `gemini-2.5-pro`, `gemini-2.0-flash-lite`, etc. Goes directly through `google-genai`; no LiteLLM dependency.

Drop-in peer of [`openai_llm`](../openai_llm/README.md) and [`anthropic_llm`](../anthropic_llm/README.md). Field shape matches so you can swap providers by changing `type:` and the api-key env var.

For multi-vendor / model-switching workflows, use [`litellm_inference_asset`](../litellm_inference_asset/README.md) instead.

## Required packages

```
dagster>=1.8.0
pandas>=1.5.0
google-genai>=0.3.0
```

## Required env var

```bash
GEMINI_API_KEY=...      # or GOOGLE_API_KEY (component falls back)
```

Get a key at <https://aistudio.google.com/app/apikey>.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_name` | yes | — | Output asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key — one row per Gemini call |
| `api_key_env_var` | no | `GEMINI_API_KEY` | Env var holding the API key |
| `text_model` | no | `gemini-2.5-flash` | Gemini text model id |
| `system_prompt` | no | — | System instruction sent on every call |
| `user_prompt_template` | one of these required | — | Static template with `{column}` placeholders |
| `input_column` | one of these required | — | Column whose value becomes the per-row prompt |
| `output_column` | no | `gemini_response` | Column for the model's text response |
| `max_output_tokens` | no | `1024` | Max output tokens per response |
| `temperature` | no | `0.0` | Temperature (0.0–2.0) |
| `top_p` / `top_k` | no | — | Optional sampling parameters |
| `rate_limit_delay` | no | `0.2` | Seconds between API calls |
| `max_retries` | no | `3` | Retries on transient errors (5xx, 429) |
| `description` / `group_name` / `deps` / `tags` / `owners` | no | — | Standard Dagster asset attributes |
| `partition_*` | no | — | Canonical registry partition shape |

## Common Gemini text models

Run `client.models.list()` against your key for the live set, or pick one of:

| Model | Strength | Notes |
|---|---|---|
| `gemini-2.5-flash` | Fast, cheap (default) | Best price/perf for most jobs |
| `gemini-2.5-pro` | Most capable | Long-context analysis, complex reasoning |
| `gemini-2.0-flash-lite` | Cheapest | Bulk classification / summarization |
| `gemini-flash-latest` | Auto-track latest flash | Stays on the freshest minor version |
| `gemini-pro-latest` | Auto-track latest pro | Same, for the pro tier |
| `gemini-3-pro-preview` | Preview of next-gen | Use only if Google's preview tier is enabled on your key |

## Failed rows

If a row fails (404 model not found, 429 quota exhausted, 5xx after retries, safety blocked), the component:

- Writes `None` into `output_column` for that row.
- Adds an `<output_column>_error` column with the error message.
- Logs an actionable message (e.g. "Set `text_model:` to a current id" on 404, "Enable billing or wait" on 429).
- Surfaces `model_not_found_count` / `quota_exhausted_count` and a `hint` in the asset metadata.

The asset still materializes — one bad row doesn't fail the whole job.

## Example

```yaml
type: dagster_component_templates.GeminiLLMComponent
attributes:
  asset_name: ticket_summaries
  upstream_asset_key: support_tickets
  api_key_env_var: GEMINI_API_KEY
  text_model: gemini-2.5-flash
  system_prompt: "You are a customer-support analyst. Output a one-sentence summary under 20 words. No preamble."
  input_column: ticket_text
  output_column: summary
  max_output_tokens: 100
  temperature: 0.0
  group_name: ai
```

## Related components

- `openai_llm` — same field shape, OpenAI provider.
- `anthropic_llm` — same field shape, Anthropic Claude provider.
- `litellm_inference_asset` — generic, multi-vendor via LiteLLM.
- `gemini_image_generation` — Gemini image gen / edit (Nano Banana).
