# OpenAI Agent

MCP client: verified with mcp 1.30.0 and 2.3.0.

> **🔑 OpenAI key required.** Set `OPENAI_API_KEY` (or override `api_key_env_var`). Also works with Azure OpenAI (set `base_url_env_var`) and OpenAI-compatible endpoints.

Single-shot LLM agent with Model Context Protocol (MCP) tool support, using the **OpenAI SDK directly** — no LiteLLM dependency. Pick this over `litellm_agent` when you're an OpenAI-only shop and want one less Python dep.

For multi-vendor support via the same shape, use `litellm_agent` instead.

---

## How it works

Runs on OpenAI's **Responses API** (`client.responses.create`), not Chat Completions — switched
specifically so `hosted_code_execution` (OpenAI's hosted `code_interpreter` tool) can exist at
all; Chat Completions has no provider-hosted code execution, only caller-implemented function
calling.

1. On materialization, connects to every MCP server in `mcp_servers` (stdio / streamable-http / sse).
2. Lists all tools from each server, registers them with OpenAI as flat function-tool defs (prefixed `<server_name>__<tool_name>` to avoid cross-server collisions) — the Responses API's tool shape has no nested `function` wrapper, unlike Chat Completions'.
3. Sends `input` (an accumulating list of message/`function_call`/`function_call_output` items) + `instructions` (the system prompt) with all tool defs to the model.
4. If the model emits `function_call` items, dispatches each to the owning MCP session and appends a matching `function_call_output` item. Loops up to `max_iterations`.
5. Stops when a turn's output has no `function_call` items (the final answer).

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

## Output

A Python dict (loaded via Dagster IO manager):

| Field | Description |
|---|---|
| `final_answer` | The model's last text response. |
| `iterations` | Model calls made. |
| `tool_calls_made` | Total tool invocations. |
| `tool_call_details` | Per-call `{iteration, tool, args, result_preview, is_error}`. |
| `transcript` | The full Responses API `input` list (message/`function_call`/`function_call_output` items), accumulated across every turn. |
| `stopped_reason` | `"final_answer"` or `"max_iterations"`. |
| `model` / `mcp_servers` | What was used. |
| `combined_server_instructions` | Per-server usage guidance actually sent to the model this run (see below). `None` when no server set any. |
| `files` | Files OpenAI's hosted code_interpreter tool generated this run (see `hosted_code_execution` below). `[]` when unset or nothing was generated. |

Key fields surface as Dagster materialization metadata.

## Per-server usage guidance (`instructions`)

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

## MCP transports

A stdio server's subprocess inherits this process's own environment (tokens, `DAGSTER_*`/`ENG_*`
settings) -- `env:` only adds/overrides on top of that, it never replaces it; see
[`litellm_agent`'s README](../litellm_agent#mcp-server-config) for why that matters in production.

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

## Skills (`skill_keys`)

`skill_keys` is a separate, parallel mechanism to `mcp_servers` for the same progressive-disclosure
idea, applied to [Agent Skills](https://www.anthropic.com/news/agent-skills) instead of tools. List
one or more `SkillResourceComponent` resource keys:

```yaml
skill_keys:
  - pdf_report_skill
```

Each listed skill's `name`/`description` (resolved from its `SKILL.md` front matter) are appended to
the system prompt before the first model call — cheap, so every configured skill shows up on turn 1.
The full `SKILL.md` body is never inlined up front; it only loads if the model actually decides to
use the skill, by calling one of two tools this component adds to the same tool-calling loop
`mcp_servers` tools already go through:

- **`load_skill(name)`** — loads the skill's full `SKILL.md` text. `name` is the skill's display
  name (as shown in the listing), not the Dagster `resource_key`.
- **`read_skill_file(name, relative_path)`** — reads one bundled file from the skill's folder, by a
  path relative to it.

A `skill_keys` entry that doesn't resolve to a registered resource (or resolves to something that
isn't a `SkillResource`) raises immediately, naming the key — this is a configuration error, not a
transient one.

When `skill_keys` is set, materialization metadata includes `skills_available` (each configured
skill's `describe()`), `skills_loaded` (names actually loaded via `load_skill` during the run), and
`skill_load_count` (total `load_skill` calls, including repeats). The asset is also tagged
`dagster-community/skill` (the configured `skill_keys`, joined — Dagster's own strict tag-value
validation doesn't allow commas, so multiple keys are `-`-joined instead).

## Hosted code execution (`hosted_code_execution`)

A `skill_keys` entry that needs code execution (bundled scripts, or `SKILL.md` text that tells
the model to run one) is normally **omitted** from the plain `load_skill`/`read_skill_file` loop
above, with a warning — that loop has no code execution at all. Set `hosted_code_execution: true`
to route those skills through OpenAI's hosted `code_interpreter` tool instead:

```yaml
skill_keys: [chart_skill]
hosted_code_execution: true
hosted_code_execution_files_dir: /var/lib/dagster/openai_agent_files   # optional
```

Every code-execution skill's `SKILL.md` plus bundled files (text only — a binary bundled file is
skipped with a warning, not a crash) are uploaded via the Files API and attached to the
`code_interpreter` container's `file_ids`, so the model can read and run them directly — no
`load_skill` call needed; the files are just already there. Generated files come back in this
asset's own `files` output and materialization metadata (`hosted_code_execution_used`,
`hosted_code_execution_files`), and — when `hosted_code_execution_files_dir` is also set — are
written there too, in the same `manifest.json` shape `report_tools_server`/`anthropic_agent`'s
own `hosted_skills_files_dir` use, so `slack_reply`/`teams_reply`'s `report_files_dir` (and
`slack_chat_tools`' `upload_files`) deliver them with zero changes needed there.

A skill's `skill_kind` (on the registering `SkillResourceComponent`) can explicitly declare
`'code_execution'` instead of relying on the auto-detected heuristic, if the heuristic
misclassifies it.

**Not live-verified against a real OpenAI account in this repo's environment** — confirmed
against OpenAI's own current API reference instead: the `container`/`file_ids` request shape, the
`container_file_citation` annotation fields, and the container-files content-retrieval endpoint
are all documented, but the exact async SDK call for reading a downloaded file's bytes wasn't
pinned down to one certain shape, so that one step tries a few plausible forms rather than
assuming one. A container expires after 20 minutes of inactivity (OpenAI's own limit, not
configurable here).

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
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var holding the OpenAI API key. |
| `base_url_env_var` | `str` | — | Optional env var holding a custom base_url (Azure OpenAI / proxies / OpenAI-compatible endpoints). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Extra asset tags. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'openai']. |
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
| `model` | `str` | `"gpt-4o-mini"` | OpenAI model id (e.g. gpt-4o, gpt-4o-mini, gpt-4-turbo, o1-mini). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `system_prompt` | `str` | — | System prompt prepended to the message list. |
| `organization_env_var` | `str` | — | Optional env var holding the OpenAI organization id. |
| `temperature` | `float` | `0.0` | Sampling temperature. |
| `max_tokens` | `int` | `2048` | Max tokens per model call. |
| `reasoning_effort` | `str` | — | Reasoning-model effort level: 'low' \| 'medium' \| 'high'. Forwarded to OpenAI o1/o3/o4 reasoning models. Passed through unchanged for other OpenAI models — OpenAI returns 400 if the model doesn't accept it, so only set… _(full docs in schema.json + component README)_ |
| `max_iterations` | `int` | `10` | Max tool-call rounds before bailing. |
| `mcp_servers` | `List[MCPServerSpec]` | `list()` | List of MCP servers to expose as tools. |
| `allowed_tools` | `List[str]` | — | Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), '*' wildcards supported (fnmatch), applied across every mcp_servers entry. Default: all discovered tools. A server can expose many tools the a… _(full docs in schema.json + component README)_ |
| `skill_keys` | `List[str]` | — | SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill. |
| `hosted_code_execution` | `bool` | `false` | Enable OpenAI's hosted code_interpreter tool. A skill_keys entry that needs code execution (skill_kind='code_execution', or the auto-detected heuristic) is no longer omitted with a warning when this is true -- its SKILL… _(full docs in schema.json + component README)_ |
| `hosted_code_execution_files_dir` | `str` | — | Local directory root -- hosted_code_execution only. When set, every file the code_interpreter tool generates is ALSO written here (one subfolder per dagster/run_id, a manifest.json per run) in the exact same shape report… _(full docs in schema.json + component README)_ |
| `outcome_ledger_path` | `str` | — | Directory a ledger-aware tool server (github_pr_tools/linear_ticket_tools, its own ledger_path field pointed at this SAME directory) writes this run's consequential actions into, keyed by run_id -- read by the companion… _(full docs in schema.json + component README)_ |
| `emit_outcome_asset` | `bool` | `false` | Build a companion '{asset_name}_outcome' asset (same partitions_def as this asset) that collapses this run's outcome-ledger entries (see outcome_ledger_path) plus this asset's own final_answer into {status, filed, would_… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

[//]: # (FIELDS:END)
