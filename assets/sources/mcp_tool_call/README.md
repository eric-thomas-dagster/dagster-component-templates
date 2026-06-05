# MCP Tool Call

Deterministic, single-shot call to one MCP tool — **no LLM** in the loop.

Use when you know exactly which tool to call and what args to pass. The asset connects to one MCP server (stdio / streamable-HTTP / sse), calls the named tool, parses the response, and materializes the result. Cheap, fast, fully reproducible.

For the LLM-driven version where the model picks the tool + args, see `litellm_agent` / `openai_agent` / `anthropic_agent` / `gemini_agent`.

---

## Why this exists

MCP servers expose two primitives — **tools** (POST-like, execute code) and **resources** (GET-like, read by URI). They're often discussed in the context of agents, but plenty of useful work is just *"call this tool with these args, on a schedule"* — at which point the LLM is pure overhead.

This component turns any MCP server into a normal Dagster source: a partitioned, freshness-policied, retry-policied asset that just runs the tool.

Pairs naturally with the agent components — same `server` config block, same headers/env-var conventions.

---

## Args templating

String values inside `tool_args` get these substitutions at materialization time:

| Token | Value |
|---|---|
| `{run_id}` | Current Dagster run id |
| `{partition_key}` | Current partition (single-partition) |
| `{partition_keys.<dim>}` | Specific axis of a multi-dim partition |

Example:

```yaml
tool_args:
  limit: 100                                  # number, passes through
  deployment_name: prod                       # string, no template — passes through
  created_after: "{partition_key}T00:00:00Z"  # templated
  region: "{partition_keys.region}"           # multi-dim templated
  trace_id: "dag-{run_id}"                    # combo
```

Only string leaves are touched — numbers / bools / nested objects pass through unchanged.

---

## Parse modes

| `parse_as` | Behavior |
|---|---|
| `auto` (default) | Try JSON; if parse fails, fall back to raw text. Output kind is `json` or `text`. |
| `json` | Require JSON. Raise if the response doesn't parse. |
| `text` | Return the raw text response. |

The parsed value is the asset's output (loaded by your IO manager). Useful for piping into `summarize`, `filter`, `dataframe_join`, `dataframe_to_csv`, etc.

---

## Standard Dagster patterns supported

| | |
|---|---|
| Partitions | `partition_type` + `partition_start` / `partition_values` / `dynamic_partition_name`, or `partition_dimensions` for multi-axis |
| Freshness | `freshness_max_lag_minutes` + `freshness_cron` |
| Retry | `retry_policy_max_retries` + `retry_policy_delay_seconds` + `retry_policy_backoff` |
| Kinds | `kinds` (defaults to `['mcp']`) |
| Owners / Tags / Description / Deps | `owners`, `asset_tags`, `description`, `deps` |

---

## Use cases

- **Daily Dagster+ snapshot** — `tool_name: list_runs`, partitioned daily, args use `{partition_key}` to filter by day.
- **MCP as universal source** — any system with an MCP server (GitHub, Jira, internal tools) becomes a Dagster asset without a custom integration.
- **MCP as sink** — `launch_run`, `create_issue`, `terminate_run` etc. triggered deterministically from upstream Dagster events.
- **Scheduled report fetch** — connect to a remote MCP, call `get_metrics`, materialize the result.

---

## Failure semantics

- MCP server returns `isError=true` → asset raises (so downstream + alerts behave normally).
- Network / connection failure → asset raises (use `retry_policy_*` to handle transient ones).
- Non-JSON response with `parse_as: json` → asset raises.
- Non-JSON response with `parse_as: auto` → asset returns the raw string and `result_kind = text` in metadata.
