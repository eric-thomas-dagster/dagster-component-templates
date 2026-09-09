# PurviewExportJobComponent

Op-shaped job that **walks the live Dagster asset graph** on each run
and pushes lineage to **Microsoft Purview Data Map** via the Apache
Atlas v2 entity bulk API.

Each run produces:

- **1 Atlas `DataSet` entity per Dagster asset**
- **1 Atlas `Process` entity per asset-graph edge** — with `inputs` / `outputs` pointing at the DataSets

## When to use this vs `lineage_to_purview`

| Component | Shape | Use case |
|---|---|---|
| `lineage_graph_extractor` + `lineage_to_purview` | **3-asset chain** | Lineage as a first-class Dagster asset. Automation-condition-driven pushes when upstream changes. |
| `PurviewExportJobComponent` (this) | **single op-job** | Scheduled catalog sync; no asset overhead. Usually the better default for "sync my Dagster asset graph to Purview nightly." |

## YAML example

```yaml
type: dagster_component_templates.PurviewExportJobComponent
attributes:
  job_name: sync_dagster_lineage_to_purview
  schedule: "0 3 * * *"
  default_status: RUNNING
  catalog_url: https://acme.purview.azure.com
  api_token_env: PURVIEW_ACCESS_TOKEN
  only_export_on_change: true
  fail_on_catalog_error: true
```

## Required env vars

```bash
PURVIEW_ACCESS_TOKEN=...             # Azure AD bearer (obtain via az account get-access-token --resource https://purview.azure.net)

# Optional — controls qualifiedName prefix:
DAGSTER_DEPLOYMENT=prod              # default "local"
```

## Behavior

1. Walks the asset graph.
2. Hashes the payload; skips push when unchanged from prior run.
3. Transforms to Atlas v2 entity format (DataSets + Process lineage).
4. POSTs to `{catalog_url}/datamap/api/atlas/v2/entity/bulk` with the bearer token.
5. Tags the run with the payload hash for next-run change detection.
