# Inference Provider A/B Test

Run the same prompt through N LLM providers side-by-side. Each provider's response becomes a first-class Dagster asset with cost, latency, and token metadata — so "should we go local" becomes an empirical query on asset history + Insights, not a benchmarking spreadsheet.

## When to use

- Deciding between LLM providers (OpenAI vs Anthropic vs local Ollama) for a specific workload.
- Justifying a provider swap with real numbers (cost drop + quality delta) instead of vendor pitches.
- Feeding branch-deploy gates: "block promotion if the winner's quality drops below threshold X."

## Composition

Pairs with two other components to close the loop:

```
InferenceProviderABTestComponent
     ├── candidate: {prefix}_gpt_4o_mini     ← one asset per provider,
     ├── candidate: {prefix}_claude_haiku    ← cost + latency in metadata
     └── candidate: {prefix}_qwen_local
                    │
                    ▼
ProviderABEvaluatorComponent
     └── {prefix}_scored                     ← LLM-as-judge quality per provider
                    │
                    ▼
InferenceCostReportComponent
     └── {prefix}_report                     ← "here's what going local saves"
                                              (time-series in Insights)
```

## Fields

- **`asset_name_prefix`** *(required, string)* — Prefix for the emitted assets. Each provider produces `{prefix}_{alias}`.
- **`prompt`** *(required, object)* — Prompt source. Shapes: `{kind: literal, text: '...'}` | `{kind: file, path: '...'}` | `{kind: url, url: '...'}`. All string fields are `{partition_key}`-templated.
- **`providers`** *(required, array)* — LLM providers to compare. Each: `{alias, model, [api_key_env_var, api_base_env_var, system_prompt, temperature, max_tokens, cost_per_1k_tokens_override]}`. `alias` becomes the asset name suffix. `model` is LiteLLM-compatible.
- **`system_prompt`** *(optional, string)* — Default system prompt.
- **`temperature`** *(optional, number, default 0.1)* — Default temperature.
- **`max_tokens`** *(optional, integer, default 500)* — Default max_tokens.
- **`partition_type`** / **`partition_start`** / **`partition_values`** / **`dynamic_partition_name`** / **`partition_dimensions`** *(optional)* — Full partition support. Use with a per-prompt dataset for scale demos.

## Provider config

Any LiteLLM-supported provider — 250+ options. Common shapes:

```yaml
# OpenAI
- alias: gpt_4o_mini
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY

# Anthropic
- alias: claude_haiku
  model: claude-3-5-haiku-latest
  api_key_env_var: ANTHROPIC_API_KEY

# Local Ollama
- alias: qwen_local
  model: ollama/qwen2.5:14b
  api_base_env_var: OLLAMA_URL          # http://localhost:11434
  cost_per_1k_tokens_override: 0.0      # local = "free" (or your $/GPU-hour equiv)

# vLLM / LM Studio / TGI (OpenAI-compatible endpoint)
- alias: mistral_vllm
  model: openai/mistralai/Mistral-7B-Instruct-v0.3
  api_base_env_var: VLLM_URL

# Bedrock, Gemini, Groq, Cohere, ... all supported via LiteLLM prefixes
- alias: claude_bedrock
  model: bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0
```

## Emitted metadata (per candidate asset)

- `provider_alias`, `model`
- `cost_usd` (from `litellm.completion_cost` OR `cost_per_1k_tokens_override × tokens_total`)
- `latency_ms`
- `tokens_in`, `tokens_out`, `tokens_total`
- `preview` (markdown-rendered first 600 chars of the response)
- Full response text in the asset payload as `text` / `content`

## Failure isolation

One provider erroring doesn't tank the whole A/B. The failing candidate emits with `status: failed` + error text in metadata; other candidates materialize normally. Downstream evaluator + report handle missing candidates gracefully.

## Example

```yaml
type: dagster_community_components.InferenceProviderABTestComponent
attributes:
  asset_name_prefix: triage_ab

  prompt:
    kind: literal
    text: "Triage this GitHub issue and classify..."

  providers:
    - alias: gpt_4o_mini
      model: gpt-4o-mini
      api_key_env_var: OPENAI_API_KEY

    - alias: claude_haiku
      model: claude-3-5-haiku-latest
      api_key_env_var: ANTHROPIC_API_KEY

    - alias: qwen_local
      model: ollama/qwen2.5:14b
      api_base_env_var: OLLAMA_URL
      cost_per_1k_tokens_override: 0.0

  system_prompt: "You are a rigorous triage assistant."
  temperature: 0.0
  max_tokens: 200
```
