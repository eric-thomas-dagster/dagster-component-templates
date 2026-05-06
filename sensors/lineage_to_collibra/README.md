# Lineage → Collibra

Sensor that exports the Dagster asset lineage graph to **collibra**.
Hashes the graph structure (nodes + edges) and only pushes when lineage
actually changes — not on every sensor tick.

## Companion components

The full lineage_to_<catalog> family in the registry:

- `lineage_to_alation`
- `lineage_to_collibra`
- `lineage_to_datahub`
- `lineage_to_openlineage` (Marquez, Atlan, Astronomer Observe)
- `lineage_to_purview`
- `lineage_to_webhook` (any HTTP endpoint)
- `lineage_to_file` (local JSON, for demos / audit logs)

Each shares the same source-system identity + change-detection cursor.
Only the per-catalog transform + push functions differ.

## Required env vars

| Var | Value |
|---|---|
| `COLLIBRA_API_TOKEN` | API token / OAuth bearer for collibra |
| `DAGSTER_PLUS_TOKEN` | (only when `scope: deployment`) Dagster+ user token |

## Scope

- `scope: code_location` (default) — exports the lineage of the current
  code location only. Always available.
- `scope: deployment` — queries the Dagster+ GraphQL API for the
  full deployment-wide graph across ALL code locations. Falls back to
  code-location scope if the API is unavailable.

## Demo mode

Set `demo_mode: true` and the sensor writes the transformed payload to a
local JSON file instead of POSTing to the catalog. Lets you preview the
exact payload shape without needing live catalog credentials.

```yaml
attributes:
  demo_mode: true
  demo_export_path: data/exports/lineage_to_collibra.json
```
