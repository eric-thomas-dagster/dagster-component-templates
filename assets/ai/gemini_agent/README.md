# Gemini Agent

MCP client: verified with mcp 1.30.0 and 2.3.0.

> **🔑 Gemini key required.** Set `GEMINI_API_KEY` (or override `api_key_env_var`). Uses the new `google-genai` SDK (not the deprecated `google-generativeai`).

Single-shot LLM agent with Model Context Protocol (MCP) tool support, using the **Google Gen AI SDK directly** — no LiteLLM dependency. Pick this when you're a Gemini-only shop and want one less Python dep.

## How it works

Standard tool-calling loop adapted to Gemini's content/parts shape:

1. Connect to every MCP server in `mcp_servers` (stdio / streamable-http / sse).
2. List tools from each, wrap them in a single `Tool(function_declarations=[…])` (Gemini's convention — one Tool per registry).
3. Send the prompt with `system_instruction` + `tools` via `client.aio.models.generate_content`.
4. Response is a list of `Part`s — each part has either `text` or `function_call`. Dispatch every `function_call` to its MCP session.
5. Tool results go back as a single `user` Content with `Part.from_function_response` blocks (Gemini's convention).
6. Loop until the model returns text without function calls, or `max_iterations`.

## `thinking_budget` — read this if outputs are getting cut off

On Gemini 2.5+, thinking tokens come out of `max_output_tokens`. If the model decides to think for 1500 tokens and your `max_output_tokens` is 2048, you have only ~550 tokens left for the answer + tool calls — and the output silently truncates.

This component defaults `thinking_budget=0` (no thinking) because the **iteration loop already gives the model multi-step reasoning** — each tool round is itself a planning step. You rarely need both.

If your task really needs deeper reasoning per turn (e.g. complex SQL synthesis), raise it explicitly:

```yaml
thinking_budget: 1024     # ¼ of max_output_tokens is a safe ratio
max_output_tokens: 4096   # bump this too if you do
```

## Output

Same dict shape as `litellm_agent` / `openai_agent` / `anthropic_agent`:

```python
{
    "final_answer": "...",
    "iterations": 3,
    "tool_calls_made": 2,
    "tool_call_details": [...],
    "transcript": [...],
    "stopped_reason": "final_answer" | "max_iterations",
    "model": "gemini-2.5-flash",
    "mcp_servers": ["dgp"],
}
```

## MCP transports

Identical config to the other native agents:

```yaml
mcp_servers:
  - name: fs
    type: stdio
    command: [npx, -y, "@modelcontextprotocol/server-filesystem", /tmp]

  - name: dgp
    type: http
    url: https://mcp.agent.dagster.cloud/mcp/
    headers:
      Dagster-Cloud-Organization: my-org
    headers_env:
      Authorization: DAGSTER_PLUS_BEARER
```

## Skills (`skill_keys`)

Alongside MCP servers, this agent can also be given **skills** — Anthropic's Agent
Skills format (a `SKILL.md` with YAML front matter + markdown body, optionally with
bundled reference files), registered once via `resources/skill_resource`'s
`SkillResourceComponent` and referenced here by `resource_key`:

```yaml
skill_keys:
  - pdf_report_skill
```

Each listed skill's `name`/`description` are injected into the agent's system prompt up
front (cheap, no I/O) so the model knows what's available on turn 1. The full `SKILL.md`
body is only fetched if the model actually decides to use a skill — the same
progressive-disclosure shape Claude Code's own Skill tool uses, not inlined into every
prompt. Two tools are added to the **same** function-declaration list the MCP tools go
through, so the model can interleave calls to either on any turn:

- **`load_skill(name)`** — loads a configured skill's full `SKILL.md` content by its
  display name (the name shown in the listing, not the Dagster `resource_key`).
- **`read_skill_file(name, relative_path)`** — reads one bundled file from a skill's
  folder by a path relative to it.

A `skill_keys` entry that doesn't resolve to a registered resource (or resolves to a
resource that isn't a `SkillResource`) raises immediately, naming the key — Dagster only
binds cross-component resources at run time, so this is the earliest point this
component's own code can check it.

Output metadata adds, only when `skill_keys` is configured:

- `skills_available` — each configured skill's `describe()` (key/name/description).
- `skills_loaded` — the display names actually loaded via `load_skill` this run.
- `skill_load_count` — how many times `load_skill` was called (repeat loads of the same
  skill count too; `read_skill_file` calls don't).

The asset is also tagged `dagster-community/skill` with the configured `skill_keys`
(sanitized to Dagster's strict tag-value charset).

## When to use vs `openai_agent` / `anthropic_agent` / `litellm_agent`

- **`gemini_agent`** — Gemini-only. `google-genai`+`mcp` deps only. Cheapest tier (gemini-2.5-flash) has a generous free quota.
- **`openai_agent`** — OpenAI / Azure OpenAI. `openai`+`mcp`.
- **`anthropic_agent`** — Claude. `anthropic`+`mcp`.
- **`litellm_agent`** — Multi-vendor + model routing + fallbacks. `litellm`+`mcp`.

## When NOT to use

| Task | Right component |
|---|---|
| Per-row DataFrame enrichment via chat completion | `gemini_llm` |
| Per-row single tool/function call (no loop) | `litellm_function_calling` |
| RAG over a corpus | `rag_pipeline` |

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
| `api_key_env_var` | `str` | `"GEMINI_API_KEY"` | Env var with the Gemini API key. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Extra asset tags. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'gemini']. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys. |

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
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries. |
| `retry_policy_backoff` | `str` | `"exponential"` | 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | `"gemini-2.5-flash"` | Gemini model id (e.g. gemini-2.5-pro, gemini-2.5-flash, gemini-2.5-flash-lite). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `system_prompt` | `str` | — | System instruction. |
| `temperature` | `float` | `0.0` | Sampling temperature. |
| `max_output_tokens` | `int` | `2048` | Max output tokens per model call. |
| `thinking_budget` | `int` | `0` | Gemini 2.5+ thinking budget. Defaults to 0 — thinking tokens come out of max_output_tokens and can silently truncate short outputs. Raise (e.g. 1024) only if your prompt needs multi-step reasoning beyond what the iterati… _(full docs in schema.json + component README)_ |
| `reasoning_effort` | `str` | — | Gemini 2.5+ reasoning effort: 'low' \| 'medium' \| 'high'. Applied via the underlying `thinking_config.include_thoughts` + implicit budget scaling on models that support it. Prefer explicit `thinking_budget` for fine-gra… _(full docs in schema.json + component README)_ |
| `max_iterations` | `int` | `10` | Max tool-call rounds. |
| `mcp_servers` | `List[MCPServerSpec]` | `list()` | MCP servers to expose as tools. |
| `allowed_tools` | `List[str]` | — | Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), '*' wildcards supported (fnmatch), applied across every mcp_servers entry. Default: all discovered tools. A server can expose many tools the a… _(full docs in schema.json + component README)_ |
| `skill_keys` | `List[str]` | — | SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

[//]: # (FIELDS:END)
