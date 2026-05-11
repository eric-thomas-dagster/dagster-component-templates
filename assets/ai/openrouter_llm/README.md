# OpenRouter LLM

Per-row LLM inference via [OpenRouter](https://openrouter.ai) — a hosted LLM gateway. **One API key, 100+ models** from Anthropic, OpenAI, Google, Meta/Llama, Mistral, DeepSeek, xAI, Cohere, with automatic fallback, caching, and unified billing.

Drop-in shape parallel to `openai_llm` / `anthropic_llm` / `gemini_llm` / `groq_llm` — native single-integration, but multi-vendor backend.

```yaml
type: dagster_component_templates.OpenRouterLLMComponent
attributes:
  asset_name: ticket_summaries
  upstream_asset_key: support_tickets
  api_key_env_var: OPENROUTER_API_KEY
  text_model: anthropic/claude-haiku-4-5
  input_column: ticket_text
  output_column: summary
```

## Model id format

OpenRouter ids are `<provider>/<model>`. Browse + pricing at https://openrouter.ai/models.

| Example id | Underlying provider |
|---|---|
| `anthropic/claude-haiku-4-5` | Anthropic |
| `openai/gpt-4o-mini` | OpenAI |
| `google/gemini-2.5-flash` | Google |
| `meta-llama/llama-3.3-70b-instruct` | Together / Cerebras / etc. (OpenRouter picks the cheapest available) |
| `deepseek/deepseek-chat-v3` | DeepSeek |
| `mistralai/mistral-large-latest` | Mistral |
| `x-ai/grok-2-1212` | xAI |

## Routing preferences

OpenRouter normally picks the cheapest available provider for a given model. Override with `provider_preferences`:

```yaml
attributes:
  text_model: meta-llama/llama-3.3-70b-instruct
  provider_preferences:
    order: [Together, Cerebras]    # try Together first, then Cerebras
    allow_fallbacks: true
```

## When to pick OpenRouter

| Pattern | Component |
|---|---|
| **Want one key for everything** | `openrouter_llm` |
| Want native speed of a single vendor | `openai_llm` / `anthropic_llm` / `gemini_llm` / `groq_llm` |
| Want side-by-side A/B comparison | `litellm_inference_asset` (multi-provider router, local routing) |
| Want OpenRouter's fallback / caching / unified-billing layer | `openrouter_llm` |

## Required env var

```bash
export OPENROUTER_API_KEY=sk-or-...
```

Get a key at https://openrouter.ai/keys ($1 minimum top-up).

## Sister components

- `openai_llm`, `anthropic_llm`, `gemini_llm`, `groq_llm` — native single-vendor peers.
- `litellm_inference_asset` — multi-vendor router via the LiteLLM Python library (local routing, not hosted).
