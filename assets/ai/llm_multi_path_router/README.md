# llm_multi_path_router

**Router agent as a single graph-backed multi-asset.** Steps live as ops in the run view; the branches the classifier picks per case land as separate Dagster assets. The alternative when `iterative_supervisor_agent`'s N-assets-per-case shape is more graph clutter than signal.

**How it works:**

- **One asset per case** (per partition). The ReAct loop lives *inside* the compute — each iteration is a `plan_step_N` op that shows up in the run view (not the asset graph). Any step declaring `done` short-circuits the rest.
- **Multi-output branches.** A `classify_and_emit` op at the end picks which of the declared `outputs:` apply to this case and emits only those. Every emitted output carries the full trajectory in materialization metadata.
- **Downstream lineage stays honest.** A sink downstream of `voucher_issued` only shows partitions for cases where the agent actually issued a voucher.

**When to reach for this vs `iterative_supervisor_agent`:**

| You want... | Component |
|---|---|
| Each ReAct step as its own asset (per-step re-runs, per-step lineage) | `iterative_supervisor_agent` |
| One asset per case; steps in the run view; multiple downstream branches | `llm_multi_path_router` (this) |
| Multiple parallel specialist LLM personas | `supervisor_agent` |
| Real tool-calling with MCP servers | `openai_agent` |

**Task template.** `task_template` uses `str.format()` against the per-partition upstream row, so `{passenger}`, `{flight}`, `{baggage_id}` etc. substitute the row values. The partition_key is also available as `{partition_key}`.

**Bounded tools = safety.** The planner picks by name from the declared `tools:` list. Unknown tools raise a validation error, not a hallucinated function call.

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Base name for the router graph (used in op naming). Emitted assets are the `outputs:` names below. |
| `upstream_asset_key` | `str` | Upstream asset providing the per-case DataFrame. Filtered by partition_static_column to a single row per partition. |
| `task_template` | `str` | Task template the planner sees per case. `{col_name}` substitutes upstream row values; `{partition_key}` also available. |
| `tools` | `List[{name, description, system_message}]` | Bounded tool set the planner picks from at each step. |
| `outputs` | `List[{name, description, kinds?}]` | Downstream branches. Each becomes its own Dagster asset, emitted only when the classifier picks it for a case. |

### LLM

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | `"gpt-4o-mini"` | OpenAI-compatible chat model — powers planner + tools + classifier. |
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var holding the LLM API key. |
| `api_base_env_var` | `str` | — | Env var for an OpenAI-compatible base URL (Ollama, Vercel AI Gateway, etc.). |
| `temperature` | `float` | `0.0` | Lower is better for tool-picking discipline. |
| `planner_max_tokens` | `int` | `400` | Max tokens per planner turn. |
| `tool_max_tokens` | `int` | `500` | Max tokens per tool LLM response. |
| `classifier_max_tokens` | `int` | `400` | Max tokens for the final classifier response. |
| `classifier_system_message` | `str` | — | Override for the classifier's system prompt. |
| `max_iterations` | `int` | `5` | Max ReAct steps (= number of plan_step ops in the graph). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Asset group (applied to every emitted output). |
| `owners` | `List[str]` | — | Asset owners. |
| `tags` | `Dict[str, str]` | — | Additional key-value tags. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | `'daily'` \| `'weekly'` \| `'monthly'` \| `'hourly'` \| `'static'` \| `'dynamic'` \| `None` |
| `partition_start` | `str` | — | ISO date for time-based partitions. |
| `partition_values` | `str` | — | Comma-separated static values, e.g. `"c1,c2,c3"`. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |
| `partition_static_column` | `str` | — | Upstream column to filter for the current partition (e.g. `case_id`). |

## Related components

- [`iterative_supervisor_agent`](../iterative_supervisor_agent) — N-assets-per-case shape (per-step lineage).
- [`supervisor_agent`](../supervisor_agent) — planner + parallel specialist tools.
- [`openai_agent`](../openai_agent) — single-asset agent with real MCP tools.
- [`human_approval_gate`](../human_approval_gate) — commonly gates a router's `escalation` branch.
