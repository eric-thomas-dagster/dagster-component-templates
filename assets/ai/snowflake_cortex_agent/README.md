# Snowflake Cortex Agent

> **🔑 Snowflake Cortex required.** Needs an account with Cortex enabled and a PAT or OAuth bearer token. Set `SNOWFLAKE_ACCOUNT_URL` + `SNOWFLAKE_PAT`.

Single-shot LLM agent backed by [Snowflake Cortex COMPLETE](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) — same MCP tool-calling loop as our other `*_agent` components, just routed through Snowflake's `/api/v2/cortex/inference:complete` REST endpoint instead of a vendor SDK.

## Why this exists

If you're already paying for Snowflake credits, running your agent loops inside Snowflake gets you:

- **Same governance and audit** as your data — no separate model-vendor key to manage.
- **Models served on the same account** as the warehouse the data sits on (lower network hops, especially with private connectivity).
- **Per-query Cortex cost reporting** — falls through to your existing Snowflake credit dashboards.

Pairs naturally with the existing two Snowflake Cortex components:

| Component | Shape |
|---|---|
| `snowflake_cortex_asset` | Per-row Cortex SQL function on a table (`COMPLETE`, `SUMMARIZE`, `SENTIMENT`, …) |
| `snowflake_cortex_search` | Managed RAG / vector search against a Cortex Search Service |
| **`snowflake_cortex_agent` ← this** | Single-shot agent loop with MCP tools, Cortex as the LLM |

## How it works

1. On materialization, connects to every MCP server in `mcp_servers` and registers their tools as OpenAI-format function definitions.
2. Sends the system + user prompt to `POST /api/v2/cortex/inference:complete` with auth header `Authorization: Bearer <token>` + `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN` (the PAT path).
3. Cortex COMPLETE response shape matches OpenAI's — `choices[0].message.tool_calls`. We dispatch each tool call to its MCP session, append the result as `role: tool`, loop.
4. Stops when the model returns plain text or `max_iterations` is hit.

## Output

Same dict shape as the other `*_agent` components — `final_answer`, `iterations`, `tool_calls_made`, `tool_call_details`, `transcript`, `stopped_reason`, `model`, `mcp_servers`.

## Example

```yaml
type: dagster_component_templates.SnowflakeCortexAgentComponent
attributes:
  asset_name: dagster_plus_cortex_agent
  prompt: "Summarize the last 5 runs in this Dagster+ deployment."
  model: claude-3-5-sonnet
  account_url_env_var: SNOWFLAKE_ACCOUNT_URL
  auth_token_env_var: SNOWFLAKE_PAT
  max_iterations: 8
  mcp_servers:
    - name: dgp
      type: http
      url: https://mcp.agent.dagster.cloud/mcp/
      headers:
        Dagster-Cloud-Organization: my-org
      headers_env:
        Authorization: DAGSTER_PLUS_BEARER
  group_name: ai
```

## Validation

Currently **code-validated** (loads cleanly + `dg check defs` passes) — full live validation needs a Snowflake account with Cortex enabled. The HTTP+JSON request/response shape follows Snowflake's public Cortex Inference API docs and mirrors the working live-validated OpenAI / Anthropic / Gemini agents.

## When NOT to use this

| Use case | Right component |
|---|---|
| Per-row Cortex SQL (`COMPLETE`, `SUMMARIZE`, etc.) on a table | `snowflake_cortex_asset` |
| Managed RAG against a Cortex Search Service | `snowflake_cortex_search` |
| Multi-vendor agent (LiteLLM-routed) | `litellm_agent` |
| Vendor-specific natives (no Snowflake) | `openai_agent` / `anthropic_agent` / `gemini_agent` |
