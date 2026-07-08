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

Same as `catalog_agent` — `task`, `include_ids` / `include_categories` / `include_tags`, `model`, `max_iterations`, etc. — with one addition:

- `defs_state: ResolvedDefsStateConfig` — where to store the plan cache. Default is `DefsStateConfigArgs.local_filesystem()`. State key is derived from a hash of the task string so different tasks get different state files.

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
