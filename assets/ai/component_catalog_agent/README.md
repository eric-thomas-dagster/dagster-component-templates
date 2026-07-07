# component_catalog_agent

The most sophisticated agent primitive in the registry. Fetches the **live component manifest** (all 900+ registered components), presents a filtered slice to a planner LLM, and executes the planner's picks by **importing the class by name → instantiating with Pydantic → calling `dg.materialize()` in-process**. Truly dynamic. Real invocation, not simulation.

## Why this shape

- **Catalog IS the bounded action space.** The manifest is the curated set. Filter tight in prod (by `include_ids`, `include_categories`, or `include_tags`) to scope what the planner can pick.
- **Real Dagster components.** Every pick becomes an actual `dg.materialize()` run — real assets, real logs, real IO manager, real outputs. No hand-authored fake tools.
- **Reflection-based instantiation.** All component classes are already importable from `dagster_community_components`. We look up the class by `component_type` from the manifest entry, instantiate with the planner's config, and execute — no `dagster-component add` needed at runtime.

## Assets emitted (3 per YAML block)

| Asset | Purpose |
|---|---|
| `<plan_asset_name>` | Planner picks `{component_id, component_type, config, reason}` from the filtered catalog. |
| `<execution_asset_name>` | For each pick: import class → instantiate → `build_defs()` → `dg.materialize()`. Emits status + output preview. |
| `<synthesis_asset_name>` | LLM synthesizes the final answer, citing each invoked component. |

## Fields

| Field | Description |
|---|---|
| `task` | The task the agent plans against. Supports `{partition_key}` / `{run_id}` substitution. |
| `manifest_url` | Where to fetch the manifest (default: this repo's `main/manifest.json`). |
| `manifest_path` | Local file path — takes precedence over `manifest_url`. |
| `include_ids` / `include_categories` / `include_tags` | Filter the catalog. |
| `max_catalog_entries` | Cap on what's shown to the planner (default `40`). |
| `max_picks` | Cap on how many components the planner may invoke. |
| `fail_on_execution_error` | If `false` (default), skip failed picks. If `true`, raise. |
| `model` / `api_key_env_var` / `temperature` | OpenAI-compatible LLM config. |
| All standard fields: `partition_type` / `freshness_*` / `retry_policy_*` / `owners` / `tags`. |

## Limitations (v1)

- **Source-style components only.** The planner should pick components that produce assets with **no external upstream deps** (like `synthetic_data_generator`, `text_embedding_asset`, `langchain_chain_asset` with a static task). Components requiring `upstream_asset_key` won't have an upstream to load from during in-process materialization.
- **Resource-requiring components will fail.** Anything expecting a Snowflake/S3/Slack resource won't have it wired at runtime — those raise. Set `fail_on_execution_error: false` (default) to skip and continue.
- **Single-asset picks.** Multi-asset components (like the other Supervisor components) will materialize but the executor only captures one output.

## Example

```yaml
type: dagster_community_components.ComponentCatalogAgentComponent
attributes:
  plan_asset_name: catalog_plan
  execution_asset_name: catalog_execution
  synthesis_asset_name: catalog_answer
  task: |
    Generate two small synthetic datasets — one of customers and one of
    products — and give me a one-paragraph description of what each looks like.
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  include_ids: [synthetic_data_generator]
  max_picks: 3
```

## Related

- `supervisor_agent` — same fan-out shape, tools are LLM personas (not the live catalog).
- `mcp_tool_picker` — tools are real MCP servers.
- `iterative_supervisor_agent` — chained tool use with per-step lineage.
