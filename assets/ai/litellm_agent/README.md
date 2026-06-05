# LiteLLM Agent

> **🔑 API key required.** Calls an LLM provider via LiteLLM. Set the env var named by `api_key_env_var` (e.g. `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`).

Single-shot LLM **agent** with [Model Context Protocol](https://modelcontextprotocol.io) (MCP) tool support. Runs the standard tool-calling loop until the model returns a final answer or `max_iterations` is hit.

Different from the `litellm_inference_asset` / `openai_llm` / `anthropic_llm` family — those are **per-row chat completion** (one prompt → one response per DataFrame row, no tool loop). This component is **single-shot agentic** (one prompt → multi-turn tool-calling loop → final answer).

---

## How it works

1. On materialization, connects to every MCP server in `mcp_servers` (stdio subprocess or SSE HTTP).
2. Lists all tools from each server and registers them with the LLM under prefixed names (`<server_name>__<tool_name>` — no cross-server collisions).
3. Sends the system + user prompt with all tool defs to the LLM.
4. If the model emits tool calls, dispatches each to the owning MCP session and feeds the result back as a `role: tool` message. Loops up to `max_iterations`.
5. Stops when the model returns plain text (no tool calls) — that's the final answer.

---

## Output

A Python dict carrying the full agent run:

| Field | Type | Description |
|---|---|---|
| `final_answer` | `str` | The model's last text response (the answer). |
| `iterations` | `int` | How many model calls were made. |
| `tool_calls_made` | `int` | Total tool invocations across all iterations. |
| `tool_call_details` | `list[dict]` | Per-call `{iteration, tool, args, result_preview, is_error}`. |
| `transcript` | `list[dict]` | Full OpenAI-shaped message list. |
| `stopped_reason` | `"final_answer" \| "max_iterations"` | Why the loop terminated. |
| `model` | `str` | Model string used. |
| `mcp_servers` | `list[str]` | Names of MCP servers that were connected. |

Materialization metadata surfaces `final_answer` (as markdown), `iterations`, `tool_calls_made`, `tool_calls` (full JSON), and `stopped_reason` directly in the Dagster UI.

---

## MCP server config

Each entry in `mcp_servers` is one server connection:

**stdio** — most common, runs a subprocess:

```yaml
- name: fs
  type: stdio
  command:
    - npx
    - -y
    - "@modelcontextprotocol/server-filesystem"
    - /tmp
  env:                 # optional, extra env vars on the subprocess
    DEBUG: "1"
```

**http** — streamable-HTTP, the modern MCP transport (what `claude mcp add --transport http` uses):

```yaml
- name: dgp
  type: http
  url: https://mcp.agent.dagster.cloud/mcp/
  headers:
    Dagster-Cloud-Organization: my-org
  headers_env:                # value read from env at materialization time
    Authorization: DAGSTER_PLUS_BEARER    # env var must hold "Bearer xyz123"
```

Use `headers` for non-secret values, `headers_env` for any header carrying a secret.

**sse** — legacy server-sent events (kept for compatibility):

```yaml
- name: time
  type: sse
  url: "http://localhost:3030/sse"
  headers: { X-Tenant: acme }
```

Reference of public MCP servers: [github.com/modelcontextprotocol/servers](https://github.com/modelcontextprotocol/servers). Includes filesystem, git, GitHub, GitLab, Postgres, SQLite, Brave Search, Puppeteer, Slack, and more. The Dagster+ MCP at `mcp.agent.dagster.cloud/mcp/` exposes 34 tools across runs, assets, deployments, alerts, issues, and metrics.

---

## When to use this vs. other components

| Use case | Component |
|---|---|
| Enrich each row of a DataFrame with an LLM | `litellm_inference_asset` |
| Per-row structured-output extraction with Pydantic | `litellm_structured_output` |
| Per-row single tool call (no loop) | `litellm_function_calling` |
| Embed a corpus + retrieve + generate | `rag_pipeline` |
| **Single agent run that uses tools to answer one question, multi-vendor** | **`litellm_agent` ← this component** |
| OpenAI-only agent (skip the LiteLLM dep) | `openai_agent` |
| Anthropic-only agent (skip the LiteLLM dep) | `anthropic_agent` |
| Gemini-only agent (skip the LiteLLM dep) | `gemini_agent` |
