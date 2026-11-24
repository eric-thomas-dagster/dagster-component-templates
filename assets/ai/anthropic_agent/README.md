# Anthropic Agent

MCP client: verified with mcp 1.30.0 and 2.3.0.

> **🔑 Anthropic key required.** Set `ANTHROPIC_API_KEY` (or override `api_key_env_var`).

Single-shot LLM agent with Model Context Protocol (MCP) tool support, using the **Anthropic SDK directly** — no LiteLLM dependency. Pick this when you're a Claude-only shop and want one less Python dep.

## How it works

Standard tool-calling loop, adapted to Anthropic's message shape:

1. Connect to every MCP server in `mcp_servers` (stdio / streamable-http / sse).
2. List tools from each, register with Anthropic as `[{name, description, input_schema}]` (prefixed by server name).
3. Send the user prompt with `system` (top-level on Anthropic) + `tools`.
4. Anthropic returns content blocks — some `text`, some `tool_use`. We dispatch every `tool_use` block to its MCP session.
5. Tool results go back as a `role: user` message with `tool_result` content blocks (Anthropic's convention, *not* `role: tool`).
6. Loop until the model returns text without any `tool_use`, or `max_iterations` is hit.

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

Identical shape across `litellm_agent` / `openai_agent` / `anthropic_agent` / `gemini_agent`:

```python
{
    "final_answer": "...",
    "iterations": 3,
    "tool_calls_made": 2,
    "tool_call_details": [...],
    "transcript": [...],
    "stopped_reason": "final_answer" | "max_iterations",
    "model": "claude-haiku-4-5-20251001",
    "mcp_servers": ["dgp"],
    "combined_server_instructions": "## dgp\n...",  # or None
}
```

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

Identical config to `openai_agent`. A stdio server's subprocess inherits this process's own
environment (tokens, `DAGSTER_*`/`ENG_*` settings) -- `env:` only adds/overrides on top of that,
it never replaces it; see [`litellm_agent`'s README](../litellm_agent#mcp-server-config) for why
that matters in production.

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

  - name: legacy
    type: sse
    url: http://localhost:3030/sse
    headers:
      X-Tenant: acme
```

## Agent Skills (`skill_keys`)

A parallel, independent mechanism to `mcp_servers`, for Anthropic's Agent Skills convention
(`SKILL.md` + optional bundled files). Register a skill once via `SkillResourceComponent`
(`resources/skill_resource`), then reference its `resource_key` from `skill_keys`:

```yaml
skill_keys:
  - pdf_report_skill
```

Each listed skill's name/description are resolved via `getattr(context.resources, key)` at the
very start of the asset's execution — same runtime-resolution convention as `server_resource_key`
above — and go straight into the system prompt up front (cheap, progressive disclosure, the same
shape Claude Code's own `Skill` tool uses). The agent gets two extra tools, merged into the same
tool list and dispatch loop the MCP tools go through:

- **`load_skill`** — `{name}`. Fetches the skill's full `SKILL.md` body. Only called once the
  model actually decides to use a skill — never inlined up front.
- **`read_skill_file`** — `{name, relative_path}`. Reads one bundled file alongside a skill's
  `SKILL.md`, by path relative to the skill's folder.

When `skill_keys` is configured, the asset also gets `skills_available` (each skill's resolved
`describe()`), `skills_loaded` (names actually loaded this run), and `skill_load_count` (every
`load_skill` call, including repeats) metadata, plus a `dagster-community/skill` tag (the
configured `skill_keys`, joined — Dagster's tag-value validation only allows
alpha-numeric/`_`/`-`/`.`, so commas are swapped for `-`).

A `skill_keys` entry that doesn't resolve to a real `SkillResource` fails loudly at the start of
the run, naming the key — this is the earliest point this component's own code can check it,
since Dagster only binds cross-component resources at run time.

## Hosted Agent Skills with code execution (`hosted_skills`)

A DIFFERENT, separate mechanism from `skill_keys` above -- `skill_keys`' `load_skill`/
`read_skill_file` only ever return text; nothing in that loop can run a script. A skill that
bundles one (see `resources/skill_resource`'s own `runnable_here`/`needs_code_execution`,
which this agent already checks to keep such a skill OUT of the `skill_keys` listing) needs
real code execution instead -- Anthropic's hosted sandbox, via `hosted_skills`:

```yaml
hosted_skills:
  - type: anthropic
    skill_id: pdf          # built-in: pdf, docx, xlsx, pptx
    version: latest
  - skill_resource_key: chart_skill   # a SkillResourceComponent(publish_as_custom_skill: true)
```

Setting `hosted_skills` adds the `code_execution` tool (`code_execution_tool_version`, default
`code_execution_20250825` -- no `anthropic-beta` header needed for any current version,
confirmed against Anthropic's own current docs) and the request's `container.skills`
automatically. Up to 20 entries (Anthropic's own limit); more raises a clear error at
`build_defs` time. The container id Anthropic returns is kept and passed back on every later
turn of the SAME run, so files Claude writes earlier in the run are still there later --
including across a `pause_turn` response (a long-running turn the API paused mid-way,
resent as-is rather than treated as a final answer or a tool-dispatch round).

A `hosted_skills` entry either names a built-in skill literally (`type: anthropic`, `skill_id`
one of `pdf`/`docx`/`xlsx`/`pptx`), or points `skill_resource_key` at a
`SkillResourceComponent` with `publish_as_custom_skill: true` -- resolved to that skill's
uploaded `anthropic_skill_id` at run time (same `getattr(context.resources, key)` convention
`skill_keys` already uses), as `type: custom`.

### Generated files

Every file Claude's code execution writes to its output directory comes back in this asset's
own output (`result["files"]`, each `{file_id, name, media_type, bytes}`) and in
`hosted_skill_files` materialization metadata (name/media_type/size, never the raw bytes).
Set `hosted_skills_files_dir` to ALSO write them to disk, one subfolder per `dagster/run_id`
with a `manifest.json` -- the EXACT shape `report_tools_server` uses -- so
[`slack_reply`](../../sinks/slack_reply)'s/[`teams_reply`](../../sinks/teams_reply)'s own
`report_files_dir` (and `slack_chat_tools`' `upload_files`) deliver these files with zero
changes needed there; point `report_tools_server` and `hosted_skills_files_dir` at the SAME
directory to have both sources land in one delivery path.

```yaml
hosted_skills_files_dir: /var/dagster/report_tools_files
```

### What this does NOT do

- Work on any non-Claude model -- `hosted_skills` is Anthropic-only, same as this whole component.
- Keep document/file contents private to your own infrastructure -- they're processed in
  Anthropic's own sandbox. Billed by execution time (Anthropic's own pricing, not this
  repo's) after a free monthly allowance; the sandbox has no internet access, so a custom
  skill that shells out to a tool like `dg` can reference its own bundled files but can't
  actually run that command.
- Let the model install additional packages at runtime -- only Anthropic's own pre-installed
  set (pandas, matplotlib, python-docx, reportlab, and others) is available.

## When to use vs `openai_agent` / `gemini_agent` / `litellm_agent`

- **`anthropic_agent`** — Claude-only shop. `anthropic`+`mcp` deps only.
- **`openai_agent`** — OpenAI / Azure OpenAI / OpenAI-compatible. `openai`+`mcp` deps only.
- **`gemini_agent`** — Gemini-only. `google-generativeai`+`mcp` deps only.
- **`litellm_agent`** — multi-vendor + model routing + fallbacks. `litellm`+`mcp` deps.

## When NOT to use

| Task | Right component |
|---|---|
| Per-row DataFrame enrichment via chat completion | `anthropic_llm` |
| Per-row single tool/function call (no loop) | `litellm_function_calling` |
| RAG over a corpus | `rag_pipeline` |

## Making sure the model actually acts (`required_tool_calls`)

Same mechanism across every `*_agent` component in this repo -- see
[`litellm_agent`'s README](../litellm_agent#making-sure-the-model-actually-acts-required_tool_calls)
for the full explanation and example. In short: `required_tool_calls` names tool(s) the model
must call at least once before finishing (a single pattern, or a list for an "any of" group); it
gets one reminder if it tries to stop without having called them, and
`on_required_tool_calls_missing` ("fail", the default, or "warn") decides what happens if it
still hasn't after that.

## Provider errors show the real reason, not an `ExceptionGroup`

Same mechanism across every `*_agent` component in this repo -- see
[`litellm_agent`'s README](../litellm_agent#provider-errors-show-the-real-reason-not-an-exceptiongroup)
for the full explanation. In short: a provider error (expired key, quota limit, rate limit) can
get wrapped by anyio's `TaskGroup` into an opaque `ExceptionGroup` during the MCP stdio client's
teardown. This component unwraps it to the real leaf error and raises a clean `dagster.Failure`
with the provider, status, and message both in the failure description and as structured
metadata, instead of leaving the real reason buried.

## Telling the agent what's deliberate (`context_notes`)

Same mechanism across every `*_agent` component in this repo -- see
[`litellm_agent`'s README](../litellm_agent#telling-the-agent-whats-deliberate-context_notes)
for the full explanation and example. In short: `context_notes` is free text appended to the
system prompt under its own heading, for saying what's deliberate in the repo/environment this
agent reviews (a demo's artificial flakiness, a test fixture that's supposed to fail) so it
doesn't get flagged as a bug.

## A readable outcome (`drafts` metadata)

Same mechanism across every `*_agent` component with `emit_outcome_asset` -- see
[`litellm_agent`'s README](../litellm_agent#a-readable-outcome-drafts-metadata) for the full
explanation. In short: the companion outcome asset's `drafts` metadata is one markdown blob with
the full title and body of everything filed or drafted in practice mode, readable straight from
the Dagster UI.

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
| `api_key_env_var` | `str` | `"ANTHROPIC_API_KEY"` | Env var with the Anthropic API key. |
| `base_url_env_var` | `str` | — | Optional env var with a custom base_url. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Extra asset tags. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'anthropic']. |
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
| `model` | `str` | `"claude-haiku-4-5-20251001"` | Anthropic model id (e.g. claude-opus-4-7, claude-sonnet-4-6, claude-haiku-4-5-20251001). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `system_prompt` | `str` | — | System prompt (Anthropic top-level param). |
| `context_notes` | `str` | — | Free text appended to the system prompt under its own heading, naming what's deliberate in the repo/environment this agent reviews -- a demo's artificial flakiness, a test fixture that's supposed to fail, a known limitat… _(full docs in schema.json + component README)_ |
| `temperature` | `float` | `0.0` | Sampling temperature. |
| `max_tokens` | `int` | `2048` | Max tokens per model call. |
| `thinking_budget` | `int` | — | Max reasoning-trace tokens for Claude thinking mode (claude-opus-4.1+, claude-sonnet-4+). When set, Anthropic returns a `thinking` block alongside the visible answer showing the model's reasoning trace. Cost implication… _(full docs in schema.json + component README)_ |
| `prompt_caching` | `bool` | `false` | Anthropic prompt caching. When true, wraps the system prompt with `cache_control: {type: ephemeral}`. First call warms the cache (~25% surcharge on that call); subsequent calls within ~5 min read from cache (~90% cheaper… _(full docs in schema.json + component README)_ |
| `max_iterations` | `int` | `10` | Max tool-call rounds. |
| `mcp_servers` | `List[MCPServerSpec]` | `list()` | MCP servers to expose as tools. |
| `allowed_tools` | `List[str]` | — | Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), '*' wildcards supported (fnmatch), applied across every mcp_servers entry. Default: all discovered tools. A server can expose many tools the a… _(full docs in schema.json + component README)_ |
| `required_tool_calls` | `List[Union[str, List[str]]]` | — | Tool name(s)/fnmatch patterns the model MUST call at least once before it's allowed to finish -- a model that writes findings and then answers 'to be filed' without actually calling the filing tool is a real, repeatedly… _(full docs in schema.json + component README)_ |
| `on_required_tool_calls_missing` | `Literal['fail', 'warn']` | `"fail"` | What happens when required_tool_calls is set and still unsatisfied after the one reminder. 'fail' (default) raises, so the run fails and the reason is in the run's own failure message -- turns 'did it act' into a guarant… _(full docs in schema.json + component README)_ |
| `skill_keys` | `List[str]` | — | SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill. |
| `hosted_skills` | `List[HostedSkillSpec]` | — | Skills Claude runs with CODE EXECUTION in Anthropic's hosted sandbox -- a DIFFERENT, separate mechanism from skill_keys (which only ever returns text, never runs anything). Enables the code_execution tool and the request… _(full docs in schema.json + component README)_ |
| `code_execution_tool_version` | `str` | `"code_execution_20250825"` | Tool `type` string for the code execution tool hosted_skills enables -- 'code_execution_20250825' (the broadly-supported default) or a later version (e.g. 'code_execution_20260120'/'code_execution_20260521') for programm… _(full docs in schema.json + component README)_ |
| `hosted_skills_files_dir` | `str` | — | Local directory root -- hosted_skills only. When set, every file Claude's code execution generates is ALSO written here (one subfolder per dagster/run_id, a manifest.json per run) in the exact same shape report_tools_ser… _(full docs in schema.json + component README)_ |
| `outcome_ledger_path` | `str` | — | Directory a ledger-aware tool server (github_pr_tools/linear_ticket_tools, its own ledger_path field pointed at this SAME directory) writes this run's consequential actions into, keyed by run_id -- read by the companion… _(full docs in schema.json + component README)_ |
| `decisions_marker` | `str` | `"decisions for you"` | Heading (case-insensitive) that starts the agent's 'decisions for a person' section at the end of its final answer. The lines under it fill the outcome asset's decisions_for_you ("none" gives nothing), and the answer tex… _(full docs in schema.json + component README)_ |
| `emit_outcome_asset` | `bool` | `false` | Build a companion '{asset_name}_outcome' asset (same partitions_def as this asset) that collapses this run's outcome-ledger entries (see outcome_ledger_path) plus this asset's own final_answer into {status, filed, would_… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

[//]: # (FIELDS:END)
