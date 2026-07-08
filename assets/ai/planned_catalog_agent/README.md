# planned_catalog_agent

**`dg.StateBackedComponent`** variant of `catalog_agent`. Runs the LLM planner + real materializations ONCE at prepare time and caches the full plan to Dagster's state store. Every subsequent load reads the cached plan and emits REAL Dagster assets — zero LLM cost.

## The Dagster+ UI story

1. In the Dagster+ UI, create a new `defs.yaml`:
   ```yaml
   type: dagster_community_components.PlannedCatalogAgentComponent
   attributes:
     task: |
       Generate synthetic orders and customers, join them,
       group by first_name, email, and month, sum total and count orders,
       and store to /tmp/orders_by_customer_month.csv.
     include_categories: [source, ingestion, transformation, sink]
     include_ids: [synthetic_data_generator]
   ```
2. Save. Code-server reload triggers `write_state_to_path` — LLM plans the pipeline once.
3. Real assets appear in the graph: `synthetic_orders`, `synthetic_customers`, `orders_with_customers`, `orders_with_month`, `orders_by_customer_month`, `orders_by_customer_month_csv`. Real names, real deps.
4. Materialize normally. Zero LLM cost on every subsequent run.
5. To re-plan: edit the task, save, code-server reloads → new plan.

## vs `catalog_agent`

| | catalog_agent | planned_catalog_agent |
|---|---|---|
| Base class | `dg.Component` | `dg.StateBackedComponent` |
| When LLM runs | Every materialization | Only at prepare time (state refresh) |
| DAG shape | `step_1 → step_2 → ... → synthesis` (wrapper assets) | Real component assets — no wrappers |
| Best for | Exploration, transparent trajectory | Production, "input task → assets appear" |
| Re-plan trigger | Change YAML, materialize | Change YAML, refresh state |
| Cost after prepare | LLM every run | Zero LLM |

## Fields

Same as `catalog_agent` — `task`, `include_ids` / `include_categories` / `include_tags`, `llm_model`, `max_iterations`, etc. — with two additions:

- `defs_state: ResolvedDefsStateConfig` — where to store the plan cache. Default is `DefsStateConfigArgs.local_filesystem()`. State key is derived from a hash of the task string so different tasks get different state files.

- `agent_hints.requires_pip` (per-component, on manifest entries) — python packages the component needs (e.g. `["sklearn"]`, `["statsmodels"]`, `["shapely"]`). The planner auto-checks `importlib.util.find_spec` for each and **filters out components whose packages aren't installed** — no configuration needed, no wasted "ModuleNotFoundError" iterations. Component authors declare their real deps once; the planner respects the actual environment.

- `available_resources: Optional[List[str]]` — external resources / credentials configured in this environment. Any component whose `agent_hints.requires_resources` includes a resource NOT in this list is filtered out of the catalog before the planner sees it. Prevents the planner from picking credentialed components you can't actually use.

  Example — user has Snowflake but no BQ/Delta:
  ```yaml
  available_resources: [snowflake_resource]
  ```
  Hides `bigquery_query_asset`, `dataframe_to_bigquery`, `delta_ingestion`, `iceberg_ingestion`, etc. Keeps `dataframe_to_snowflake` and all resource-free components (csv/json/parquet/transformations).

  Default `None` = no filtering (assume everything is available).

- `prefilter_llm: bool = False` — pre-filter the catalog via ONE cheap `gpt-4o-mini` call before the planner iterates. Reads (id + short description) for every filtered component and returns ~40 IDs it thinks the task actually needs. Live-validated: 300-component wide catalog → 41 shortlisted → 14/14 clean picks in 29s (vs 14 min without prefilter on gpt-4o). Trade-offs:
  - Adds one ~5-10s LLM call at trajectory start.
  - Costs ~$0.001 (mini pricing).
  - If it misses a component the task needs, the planner may fail. Combine with `include_ids` for guaranteed availability.
  - Highest-leverage lever when the catalog is wide and the task is narrow.
- `prefilter_max_entries: int = 40` — target count for the pre-filter's shortlist.

- `tpm_budget: Optional[int]` — OpenAI tokens-per-minute budget for a single request. When set, the component progressively trims catalog contents to fit `tpm_budget - 2000` (safety margin):
  1. Drop `anti_uses` hints (keep `side_effects` only)
  2. Trim descriptions from 300 → 150 chars
  3. Drop all hints
  4. Cut catalog entries from the tail

  **Trade-offs to understand:**
  - Lower budgets → less context per pick → **more hallucinations, more failed iterations** → often SLOWER end-to-end despite fewer tokens per call.
  - The correct primary knob for Tier-1 OpenAI accounts is `include_ids` (narrow the universe of picks); `tpm_budget` is a fallback safety net that keeps the trajectory from getting 429s.
  - OpenAI Tier reference: Tier1=30000, Tier2=500000, Tier3=5000000.
  - Default `None` = no trimming; trust the user's `include_*` filters.

  Trim decisions are persisted in the state file under `state.timing.catalog_trim_notes` so you can see what got dropped and why.

## Refreshing state

```bash
# Automatic — `dagster dev` re-runs write_state_to_path when needed.
dagster dev

# Explicit refresh
dg utils refresh-defs-state
```

## Related

- `catalog_agent` — the exploration variant (step-by-step trajectory in `dg dev`).
- `codegen_output_dir` on `catalog_agent` — the other graduation path: writes real defs.yaml files you commit to your repo.
