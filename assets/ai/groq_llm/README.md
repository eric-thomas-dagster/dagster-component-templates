# Groq LLM

Native Groq LLM inference via Groq's OpenAI-compatible API. Drop-in peer of `openai_llm` / `anthropic_llm` / `gemini_llm`.

Groq is uniquely fast — **500+ tokens/sec** on Llama-3.x / Mixtral / Gemma / DeepSeek-R1 thanks to their custom LPU hardware. Generous **free tier**: ~30 req/min for most models, no credit card required.

```yaml
type: dagster_component_templates.GroqLLMComponent
attributes:
  asset_name: ticket_summaries
  upstream_asset_key: support_tickets
  api_key_env_var: GROQ_API_KEY
  text_model: llama-3.3-70b-versatile
  system_prompt: "You are a customer-support analyst. One sentence, under 20 words."
  input_column: ticket_text
  output_column: summary
```

## Common Groq models

| Model | Strength |
|---|---|
| `llama-3.3-70b-versatile` (default) | Best general-purpose, large context |
| `llama-3.1-8b-instant` | Fastest, smallest, near-free |
| `mixtral-8x7b-32768` | 32K context, MoE architecture |
| `gemma2-9b-it` | Google's open-weights |
| `deepseek-r1-distill-llama-70b` | Reasoning model (thinks before answering) |

Full list: https://console.groq.com/docs/models

## Required env var

```bash
export GROQ_API_KEY=gsk_...
```

Get a free key at https://console.groq.com/keys (no card required).

## When to pick Groq vs other native LLM components

| Provider | Best for |
|---|---|
| **`groq_llm`** | Low latency (real-time apps), large batch jobs on a budget, open-weights models |
| `openai_llm` | gpt-4o, structured outputs, broad ecosystem |
| `anthropic_llm` | Claude (long context, prompt caching) |
| `gemini_llm` | Free Gemini quota, multimodal |
| `openrouter_llm` | One key for 100+ models across all providers |
| `litellm_inference_asset` | Multi-vendor routing, fallback, comparison |

## Sister components

- `openai_llm`, `anthropic_llm`, `gemini_llm`, `openrouter_llm` — native peers.
- `litellm_inference_asset` — generic multi-vendor router.
- `litellm_structured_output` — JSON-mode wrapper; works with Groq via `litellm` too.
