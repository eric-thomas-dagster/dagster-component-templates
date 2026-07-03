# vercel_ai_gateway_agent

Single-shot LLM agent via **Vercel AI Gateway** — an OpenAI-compatible proxy that routes to any supported provider (OpenAI, Anthropic, Google, xAI, Groq, and more) with a single Vercel API token, unified billing, automatic fallback across models, and Vercel-side observability.

## Why route through Vercel AI Gateway

- **One key, any model.** Swap `model: openai/gpt-4o` for `anthropic/claude-sonnet-4-6` or `google/gemini-2.5-pro` without changing your API key or endpoint.
- **Automatic fallback.** Set `fallback_models: [...]` — on rate-limit or provider outage, the agent retries with each in order.
- **Unified billing + observability.** All spend rolls up in your Vercel dashboard. Every call is visible in Vercel's AI Gateway analytics.
- **Same shape as native agents.** Reuses the openai SDK with a custom `base_url`; MCP tool support is identical to `openai_agent`/`anthropic_agent`.

## Example

```yaml
type: dagster_community_components.VercelAIGatewayAgentComponent
attributes:
  asset_name: research_agent
  prompt: "Summarize the top 3 news items from the last 24 hours."
  model: anthropic/claude-sonnet-4-6
  api_key_env_var: VERCEL_API_TOKEN
  max_iterations: 6

  fallback_models:
    - openai/gpt-4o
    - google/gemini-2.5-flash

  mcp_servers:
    - name: dgp
      type: http
      url: https://mcp.agent.dagster.cloud/mcp/
      headers_env:
        Authorization: DAGSTER_PLUS_BEARER
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_name` | string | Output asset name. |
| `prompt` | string | User prompt. Supports `{run_id}`, `{partition_key}`, `{partition_keys.<dim>}` substitution. |
| `system_prompt` | string | Optional system message. |
| `model` | string | Gateway model string, `<provider>/<model>`. Default `openai/gpt-4o-mini`. |
| `fallback_models` | list | Retry each on failure. |
| `api_key_env_var` | string | Vercel API token env var. Default `VERCEL_API_TOKEN`. |
| `api_base_url` | string | Default `https://ai-gateway.vercel.sh/v1`. |
| `temperature` / `max_tokens` / `max_iterations` | — | Standard controls. |
| `mcp_servers` | list | Same shape as `openai_agent` / `anthropic_agent` — stdio, http (streamable), sse. |
| `group_name` / `description` / `owners` / `asset_tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Model strings (partial catalog — see Vercel for current list)

- `openai/gpt-4o`, `openai/gpt-4o-mini`
- `anthropic/claude-sonnet-4-6`, `anthropic/claude-haiku-4-5`
- `google/gemini-2.5-pro`, `google/gemini-2.5-flash`
- `xai/grok-4`, `xai/grok-3-mini`
- `groq/llama-3.3-70b`
- `mistral/mistral-large`
- (See https://vercel.com/ai-gateway/models for the current list.)

## Output

Materialized value:

```python
{
  "final_answer": "...",
  "iterations": 3,
  "tool_calls_made": 2,
  "tool_call_details": [...],
  "transcript": [...],
  "stopped_reason": "final_answer",
  "model": "anthropic/claude-sonnet-4-6",     # model actually invoked
  "model_used": "anthropic/claude-sonnet-4-6",
  "fallback_chain": ["anthropic/claude-sonnet-4-6", "openai/gpt-4o", ...],
  "mcp_servers": ["dgp"],
}
```

Asset metadata surfaces the final answer, model requested vs used, fallback chain, tool calls, and gateway identifier.

## Auth

Create a Vercel API token at https://vercel.com/account/tokens with AI Gateway scope. The gateway auto-routes to the correct upstream provider and bills against your Vercel account.

## Requirements

```
openai>=1.0.0
mcp>=1.0.0
```

## Related

- `openai_agent` / `anthropic_agent` / `gemini_agent` — native, single-provider variants.
- `litellm_agent` — multi-provider via LiteLLM (self-hosted routing instead of Vercel-hosted).
- `langgraph_agent` — multi-step stateful graph pipeline.
