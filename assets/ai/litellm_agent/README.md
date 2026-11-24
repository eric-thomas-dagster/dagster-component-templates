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

## Prompt placeholders

`prompt` / `system_prompt` (and every stdio `mcp_servers[].env` value) support these
placeholders, substituted once per run:

| Placeholder | Value |
|---|---|
| `{run_id}` | This run's id. |
| `{partition_key}` | The run's partition key, if partitioned. |
| `{partition_keys.<dim>}` | One dimension of a multi-partition key. |
| `{tag.<key>}` | The Dagster run tag `<key>` -- any key, including slash-namespaced ones like `slack/thread_ts`. A tag that isn't set on the run leaves the placeholder untouched rather than substituting an empty string. |

`{tag.<key>}` is how a chat-triggered run (e.g. `slack_agent_request_sensor`) hands its own
identifiers into the prompt (`"Answer the Slack message {tag.slack/message_ts}..."`) **and** into
a spawned MCP tool's environment -- e.g. `env: {REQUESTER: "{tag.slack/user}"}` on a
`dagster_workflow_tools` server entry, so a write tool's authorization check gets the real
requester identity from a trusted, server-side source instead of a model-settable tool argument.

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
| `combined_server_instructions` | `str \| None` | Combined per-server usage guidance actually sent to the model this run (see "Per-server usage guidance" below). `None` when no server set any. |

Materialization metadata surfaces `final_answer` (as markdown), `iterations`, `tool_calls_made`, `tool_calls` (full JSON), `stopped_reason`, and (when set) `mcp_server_instructions` directly in the Dagster UI.

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

A stdio server's subprocess inherits this process's own environment (tokens, `DAGSTER_*`/`ENG_*`
settings, everything) -- `env:` only ADDS or overrides on top of that, it never replaces it. This
matters in production: without it, none of a Dagster+ deployment's own env vars would reach the
spawned tool server, and every tool call would fail as if those vars were never set. Don't rely
on `env:` to carry a secret, though -- a literal value there is templated into component config,
which is visible wherever that config is visible (the Dagster UI, `dg` output). A secret should
already be a real env var on the deployment (it's inherited automatically) or passed via
`headers_env` for an http/sse server.

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

### Per-server usage guidance (`instructions`)

An MCP server's tools carry no built-in knowledge of how a company wants them used (e.g. "link
each ticket as `https://app.example.com/issues?issueNumber=<number>`", "show names, not IDs",
"this server is read-only"). Set `instructions` on an `mcp_servers` entry (inline or on the
`server_resource_key`'d `MCPServerResourceComponent`) to say this once, instead of pasting it
into every agent's own `prompt`/`system_prompt`:

```yaml
mcp_servers:
  - name: tickets
    type: http
    url: https://tickets.example.com/mcp
    instructions: "Link each ticket as https://app.example.com/issues?issueNumber=<number>."
```

This text is appended to the system prompt under a `## <server name>` heading, combined with
whatever `instructions` the server itself returns in its own `initialize()` response (if it sets
one, labeled `(server-provided)`) — every agent that connects to the same server gets the same
guidance with no copy-paste, and per-agent `system_prompt` text can still add to it. The combined
text per run is both returned (`combined_server_instructions`) and surfaced in run metadata
(`mcp_server_instructions`) so it's visible what the model was actually told.

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

## Making sure the model actually acts (`required_tool_calls`)

A model that writes up findings and then answers "to be filed" without ever calling the filing
tool is a real, repeatedly observed failure mode -- prompt wording alone doesn't reliably prevent
it. `required_tool_calls` turns "did it act" from a hope into a guarantee:

```yaml
required_tool_calls:
  - dagster_plus__open_issue          # must be called at least once
  - [dagster_plus__propose_fix, dagster_plus__open_issue]  # "any of" group -- either satisfies it
on_required_tool_calls_missing: fail   # default; "warn" never raises, just reports it instead
```

Each entry is a namespaced tool name or fnmatch pattern (required), or a list of them (an "any
of" group, satisfied by calling any one). When the model tries to stop without having called
every required entry, it gets exactly ONE reminder message -- consuming one of `max_iterations`'
own rounds, not extra budget -- and a chance to call them before finishing. Whether a matched
call itself succeeded or errored doesn't matter, only whether it was attempted.

If it still hasn't called them after that: `on_required_tool_calls_missing: fail` (the default)
raises, so the run fails and the reason is in the run's own failure message. `warn` never raises;
instead the result -- and the `_outcome` companion asset, when `emit_outcome_asset` is on --
carries `required_tools_missing`, the list of entries still unsatisfied (empty when everything
required was called).

---

## Provider errors show the real reason, not an `ExceptionGroup`

A provider error (an expired key, a quota limit, a rate limit) raised while the agent loop's MCP
stdio client is still open gets wrapped by anyio's `TaskGroup` during the client's teardown --
without this fix, the step failure reads `ExceptionGroup: unhandled errors in a TaskGroup (1
sub-exception)`, with the real error buried one or more levels inside it and nothing in the
message pointing at the provider at all. This was reported from production: an OpenAI
`insufficient_quota` rejection on the very first request cost an hour of debugging before anyone
suspected it wasn't an MCP problem.

The asset body now unwraps any such group down to the real leaf error and raises a clean
`dagster.Failure` instead, with the provider, HTTP status, and message both in the failure
description and as structured metadata (`error_class`, `provider`, `status_code`,
`error_message`) -- visible on the run's own failure event, no digging through a nested group
required. An error that was never wrapped in the first place (most errors, outside the narrow
MCP-teardown window) passes through completely unchanged.

To catch a bad key or an exhausted quota *before* a real agent run hits it, pair this component
with [`model_provider`](../model_provider)'s live-connectivity check -- see that component's
README for how to wire it to a schedule and a Dagster+ alert.

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
| `required_tool_calls` | `List[Union[str, List[str]]]` | — | Tool name(s)/fnmatch patterns the model MUST call at least once before it's allowed to finish -- a model that writes findings and then answers 'to be filed' without actually calling the filing tool is a real, repeatedly… _(full docs in schema.json + component README)_ |
| `on_required_tool_calls_missing` | `Literal['fail', 'warn']` | `"fail"` | What happens when required_tool_calls is set and still unsatisfied after the one reminder. 'fail' (default) raises, so the run fails and the reason is in the run's own failure message -- turns 'did it act' into a guarant… _(full docs in schema.json + component README)_ |
| `skill_keys` | `List[str]` | — | SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill. |
| `include_reasoning` | `bool` | `false` | Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped from the transcript and final_answer; when true it is also returned as a 'reasoning' key in the result dict (all itera… _(full docs in schema.json + component README)_ |
| `outcome_ledger_path` | `str` | — | Directory a ledger-aware tool server (github_pr_tools/linear_ticket_tools, its own ledger_path field pointed at this SAME directory) writes this run's consequential actions into, keyed by run_id -- read by the companion… _(full docs in schema.json + component README)_ |
| `emit_outcome_asset` | `bool` | `false` | Build a companion '{asset_name}_outcome' asset (same partitions_def as this asset) that collapses this run's outcome-ledger entries (see outcome_ledger_path) plus this asset's own final_answer into {status, filed, would_… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

[//]: # (FIELDS:END)

## Reasoning models (Qwen3, DeepSeek-R1, ...)

Reasoning models inline their chain of thought as `<think>...</think>`. The component strips it from the model's reply *before* parsing/storing, so the output is the clean answer (non-reasoning models are unaffected). Set `include_reasoning: true` to keep the thinking: the result dict gains a `reasoning` key (all iterations joined; `null` if none) and a `reasoning` metadata entry. `ollama/...` models are routed to LiteLLM's `ollama_chat/` when MCP tools are present (the `/api/generate` route has no tool support), and `<tool_call>{...}</tool_call>` emitted as plain text is recovered into real tool calls.
