# Lineage Graph Extractor

Materialize the canonical Dagster asset lineage graph as an asset.
Pair with one or more `lineage_to_<catalog>` sink components to fan
out lineage to multiple catalogs in lock-step.

```
                    lineage_graph_extractor
                          │ (lineage_graph asset)
              ┌───────────┼───────────┐
              ▼           ▼           ▼
       lineage_to_     lineage_to_  lineage_to_
       purview        datahub        alation        ...
```

## Why this shape?

Earlier this lived as 7 standalone sensors, each rebuilding the graph
and maintaining its own change-detection cursor. Two sensors running on
slightly different schedules could push out-of-sync versions of lineage.

The asset-pipeline shape:
- **One source of truth** — `lineage_graph` materialized once per tick
- **Lock-step fan-out** — all catalog sinks see the same graph snapshot
- **Standard Dagster patterns** — auto-materialize, schedules, sensors
  all work for triggering the chain
- **Inspect in the UI** — payload + groups + edges visible as metadata

## Scope

- `code_location` (default): exports the current code location's
  asset graph via `context.repository_def.asset_graph`
- `deployment`: queries Dagster+ GraphQL for the *full* deployment graph
  across ALL code locations. Falls back to code-location scope if the
  GraphQL API is unavailable.
