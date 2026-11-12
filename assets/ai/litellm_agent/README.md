# LiteLLM Agent

MCP client: verified with mcp 1.30.0 and 2.3.0.

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

## Skills (`skill_keys`)

A PARALLEL mechanism to `mcp_servers`, for Agent Skills instead of MCP tools. Register a skill once with `SkillResourceComponent` (see `resources/skill_resource`), then reference it by `resource_key`:

```yaml
skill_keys:
  - pdf_report_skill
```

Each listed skill's name/description is resolved (at materialization time, via `context.resources`) and appended to the system prompt up front — cheap, no I/O. Two tools are added to the same tool-calling loop MCP tools go through:

- **`load_skill(name)`** — fetches the skill's full `SKILL.md` body. Only called once the model actually decides to use that skill (progressive disclosure, not inlining every skill's full instructions into every prompt).
- **`read_skill_file(name, relative_path)`** — reads one bundled file alongside the skill (a reference doc, template, script, ...), by path relative to the skill's folder.

A `skill_keys` entry that doesn't resolve to a registered resource (or resolves to something that isn't a `SkillResource`) raises immediately, naming the key — this is the earliest point this component's own code can check it, since Dagster only binds cross-component resources at run time.

When `skill_keys` is configured, output metadata gains:

| Field | Type | Description |
|---|---|---|
| `skills_available` | `json` | Each configured skill's `{key, name, description}`. |
| `skills_loaded` | `json` | Names of skills whose `load_skill` was actually called this run. |
| `skill_load_count` | `int` | Total `load_skill` calls made (every call counts, including repeat loads of the same skill — `read_skill_file` calls don't count toward this). |

The asset is also tagged `dagster-community/skill` (comma-joined `skill_keys`, with any character outside Dagster's strict tag-value allowlist swapped for `-`).

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

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name. |
| `prompt` | `str` | User prompt sent to the model on the first turn. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | — | Env var holding the provider API key. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Extra asset tags. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'litellm']. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | FreshnessPolicy max lag in minutes. |
| `freshness_cron` | `str` | — | FreshnessPolicy deadline cron schedule. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | 'daily' \| 'weekly' \| 'monthly' \| 'hourly' \| 'static' \| 'multi' \| 'dynamic'. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | ISO date for time-based partitions. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static/multi. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values} dicts. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure (transient API / network). |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries. |
| `retry_policy_backoff` | `str` | `"exponential"` | 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | `"gpt-4o-mini"` | LiteLLM model string (e.g. gpt-4o, claude-haiku-4-5-20251001, gemini/gemini-2.5-flash). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `system_prompt` | `str` | — | System prompt prepended to the message list. |
| `api_base_env_var` | `str` | — | Env var holding a custom API base URL (proxies, self-hosted). |
| `temperature` | `float` | `0.0` | Sampling temperature. |
| `max_tokens` | `int` | `2048` | Max tokens per model call. |
| `reasoning_effort` | `str` | — | Reasoning-model effort level: 'low' \| 'medium' \| 'high'. Forwarded to OpenAI o1/o3 and Gemini 2.5+ reasoning models (silently dropped for other providers). |
| `thinking_budget` | `int` | — | Max reasoning-trace tokens. Forwarded to Gemini 2.5+ (native `thinking_budget`) and Anthropic thinking mode (auto-mapped to `thinking: {type: enabled, budget_tokens: N}`). On Gemini, set 0 for short structured outputs to… _(full docs in schema.json + component README)_ |
| `prompt_caching` | `bool` | `false` | Anthropic prompt caching. When true AND the model is Anthropic-family (claude / anthropic / bedrock/anthropic.), wraps the system prompt with `cache_control: {type: ephemeral}` so subsequent runs within ~5 min hit the ca… _(full docs in schema.json + component README)_ |
| `max_iterations` | `int` | `10` | Max tool-call rounds before bailing. |
| `mcp_servers` | `List[MCPServerSpec]` | `list()` | List of MCP servers to expose as tools. Empty list = no tools (chat-only). |
| `allowed_tools` | `List[str]` | — | Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), '*' wildcards supported (fnmatch), applied across every mcp_servers entry. Default: all discovered tools. A server can expose many tools the a… _(full docs in schema.json + component README)_ |
| `skill_keys` | `List[str]` | — | SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill. |
| `include_reasoning` | `bool` | `false` | Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped from the transcript and final_answer; when true it is also returned as a 'reasoning' key in the result dict (all itera… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

[//]: # (FIELDS:END)

## Reasoning models (Qwen3, DeepSeek-R1, ...)

Reasoning models inline their chain of thought as `<think>...</think>`. The component strips it from the model's reply *before* parsing/storing, so the output is the clean answer (non-reasoning models are unaffected). Set `include_reasoning: true` to keep the thinking: the result dict gains a `reasoning` key (all iterations joined; `null` if none) and a `reasoning` metadata entry. `ollama/...` models are routed to LiteLLM's `ollama_chat/` when MCP tools are present (the `/api/generate` route has no tool support), and `<tool_call>{...}</tool_call>` emitted as plain text is recovered into real tool calls.
