# catalog_agent

Fusion of the two most sophisticated primitives:

- **Iterative** (like `iterative_supervisor_agent`) — per-step planner, N pre-declared step assets, short-circuit termination.
- **Catalog** (like `component_catalog_agent`) — picks real components from the LIVE manifest and executes them via reflection + in-process materialize.

What makes this the strongest shape yet: **schema discovery.** At each step the planner sees the ACTUAL columns + preview of every prior step's real materialized DataFrame. That means the agent can chain against customer-built data with unknown schemas — step 1 runs, agent inspects real output columns, step 2's planner picks with knowledge of the real schema.

## Assets emitted (`max_iterations + 1`)

| Asset | Purpose |
|---|---|
| `<step_asset_prefix>_1` … `<step_asset_prefix>_<max_iterations>` | Each step's output is a dict: `{plan: {iteration, done, component_id, config, reason, asset_name, output_columns, output_preview, status, error}, df: <actual DataFrame or None>}`. |
| `<synthesis_asset_name>` | Reads all steps, writes final answer. |

Static DAG shape at YAML load. Dynamic termination: whichever step declares `done` short-circuits all downstream steps to no-ops.

## The chaining mechanic

When a step's picked component uses `upstream_asset_key`, the executor:

1. Looks up prior steps' outputs by matching `asset_name`.
2. Constructs an ad-hoc source asset seeded from the prior step's actual DataFrame value.
3. Calls `dg.materialize(source + picked_component_assets)` — Dagster runs both in one graph.

Downstream steps that read from prior outputs get REAL data flowing through, not stubbed values.

## Fields

Same as `component_catalog_agent` plus `max_iterations` (default 5, range 1–15) and `step_asset_prefix` (default `"catalog_step"`).

## Example

```yaml
type: dagster_community_components.CatalogAgentComponent
attributes:
  step_asset_prefix: catalog_step
  synthesis_asset_name: catalog_final_answer
  task: |
    Do a small analytics workflow. Pick a schema, generate data,
    then filter and summarize using the real columns you discover
    from the first step. Chain via upstream_asset_key.
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  include_ids: [synthetic_data_generator, filter, summarize, dataframe_describe]
  max_iterations: 5
```

## Related

- `component_catalog_agent` — single-shot version. Planner picks all steps upfront.
- `iterative_supervisor_agent` — same iterative shape but hand-authored LLM-persona tools instead of the live catalog.
- `mcp_tool_picker` — catalog-of-MCP-tools version (single-shot).
