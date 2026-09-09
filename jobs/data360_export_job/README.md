# Data360ExportJobComponent

Op-shaped job that **walks the live Dagster asset graph** on each run
and pushes lineage to **Precisely Data360 Govern** via the Catalog
REST endpoint. Auth is OAuth2 client-credentials (unlike the other
lineage export jobs which use static bearer tokens).

Each run produces:

- **1 Data360 object per Dagster asset** — with description, `externalUrl` back to Dagster, and Dagster metadata as customProperties
- **1 lineage edge per asset-graph edge** — `lineageType: DATA_FLOW`

## When to use this vs `lineage_to_data360`

| Component | Shape | Use case |
|---|---|---|
| `lineage_graph_extractor` + `lineage_to_data360` | **3-asset chain** | Lineage as a first-class Dagster asset. Automation-condition-driven pushes when upstream changes. |
| `Data360ExportJobComponent` (this) | **single op-job** | Scheduled catalog sync; no asset overhead. Usually the better default for "sync my Dagster asset graph to Data360 nightly." |

## YAML example

```yaml
type: dagster_component_templates.Data360ExportJobComponent
attributes:
  job_name: sync_dagster_lineage_to_data360
  schedule: "0 3 * * *"
  default_status: RUNNING
  catalog_url: https://api.data.precisely.com/data360/catalog
  token_url: https://api.data.precisely.com/oauth/token
  client_id_env: PRECISELY_DIS_CLIENT_ID
  client_secret_env: PRECISELY_DIS_CLIENT_SECRET
  only_export_on_change: true
  fail_on_catalog_error: true
```

## Required env vars

```bash
PRECISELY_DIS_CLIENT_ID=...          # OAuth2 client_id
PRECISELY_DIS_CLIENT_SECRET=...      # OAuth2 client_secret

# Optional:
DAGSTER_UI_URL=https://dagster.acme.com
DAGSTER_DEPLOYMENT=prod
```

## Behavior

1. Walks the asset graph.
2. Hashes the payload; skips push when unchanged from prior run.
3. OAuth2 client-credentials → obtains a bearer token from `token_url`.
4. Transforms to Data360 format (objects + edges).
5. POSTs objects to `{catalog_url}/objects` and edges to `{catalog_url}/lineage` sequentially.
6. Tags the run with the payload hash for next-run change detection.
