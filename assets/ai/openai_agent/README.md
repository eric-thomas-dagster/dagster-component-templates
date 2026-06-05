# OpenAI Agent

> **🔑 OpenAI key required.** Set `OPENAI_API_KEY` (or override `api_key_env_var`). Also works with Azure OpenAI (set `base_url_env_var`) and OpenAI-compatible endpoints.

Single-shot LLM agent with Model Context Protocol (MCP) tool support, using the **OpenAI SDK directly** — no LiteLLM dependency. Pick this over `litellm_agent` when you're an OpenAI-only shop and want one less Python dep.

For multi-vendor support via the same shape, use `litellm_agent` instead.

---

## How it works

1. On materialization, connects to every MCP server in `mcp_servers` (stdio / streamable-http / sse).
2. Lists all tools from each server, registers them with OpenAI as function-calling defs (prefixed `<server_name>__<tool_name>` to avoid cross-server collisions).
3. Sends the system + user prompt with all tool defs to the model.
4. If the model emits tool calls, dispatches each to the owning MCP session and feeds the result back as a `role: tool` message. Loops up to `max_iterations`.
5. Stops when the model returns plain text (the final answer).

## Output

A Python dict (loaded via Dagster IO manager):

| Field | Description |
|---|---|
| `final_answer` | The model's last text response. |
| `iterations` | Model calls made. |
| `tool_calls_made` | Total tool invocations. |
| `tool_call_details` | Per-call `{iteration, tool, args, result_preview, is_error}`. |
| `transcript` | Full OpenAI-shaped message list. |
| `stopped_reason` | `"final_answer"` or `"max_iterations"`. |
| `model` / `mcp_servers` | What was used. |

Key fields surface as Dagster materialization metadata.

## MCP transports

```yaml
mcp_servers:
  # 1. stdio (subprocess) — local tools
  - name: fs
    type: stdio
    command: [npx, -y, "@modelcontextprotocol/server-filesystem", /tmp]

  # 2. http (streamable-HTTP, the modern MCP transport)
  - name: dgp
    type: http
    url: https://mcp.agent.dagster.cloud/mcp/
    headers:
      Dagster-Cloud-Organization: my-org
    headers_env:
      Authorization: DAGSTER_PLUS_BEARER   # value must include "Bearer " prefix

  # 3. sse (legacy server-sent events)
  - name: legacy_remote
    type: sse
    url: http://localhost:3030/sse
    headers:
      X-Tenant: acme
```

`headers_env` reads the header value from an env var at materialization time — use this for tokens.

## When to use vs `litellm_agent`

| | `openai_agent` | `litellm_agent` |
|---|---|---|
| Vendor lock-in | OpenAI / Azure OpenAI / OpenAI-compatible | 100+ providers |
| Python deps | `openai`, `mcp` | `litellm`, `mcp` |
| Use case | OpenAI-only shops, Azure | Multi-vendor, model routing, fallbacks |

## When NOT to use

| Task | Right component |
|---|---|
| Per-row DataFrame enrichment via chat completion | `openai_llm` |
| Per-row single tool/function call (no loop) | `litellm_function_calling` |
| RAG over a corpus | `rag_pipeline` |
