# AlationExportJobComponent

Op-shaped job that **walks the live Dagster asset graph** on each run
and pushes lineage to **Alation Data Catalog** via the Integration v2
lineage endpoint. Idempotent — Alation upserts on external_id.

Each run produces:

- **1 dataflow object per Dagster asset** — with description + Dagster UI back-link + Dagster metadata as JSON `content`
- **1 lineage path per asset-graph edge** — chained as `external → dataflow → external` so Alation renders proper lineage

## When to use this vs `lineage_to_alation`

| Component | Shape | Use case |
|---|---|---|
| `lineage_graph_extractor` + `lineage_to_alation` | **3-asset chain** | Lineage as a first-class Dagster asset. Automation-condition-driven pushes when upstream changes. |
| `AlationExportJobComponent` (this) | **single op-job** | Scheduled catalog sync; no asset overhead. Usually the better default for "sync my Dagster asset graph to Alation nightly." |

## YAML example

```yaml
type: dagster_component_templates.AlationExportJobComponent
attributes:
  job_name: sync_dagster_lineage_to_alation
  schedule: "0 3 * * *"
  default_status: RUNNING
  catalog_url: https://alation.acme.com
  api_token_env: ALATION_API_TOKEN
  only_export_on_change: true
  fail_on_catalog_error: true
```

## Required env vars

```bash
ALATION_API_TOKEN=...                # sent as TOKEN header

# Optional — used in the URL back-link on each dataflow object:
DAGSTER_UI_URL=https://dagster.acme.com
DAGSTER_DEPLOYMENT=prod
DAGSTER_ORGANIZATION=acme
```

## Behavior

1. Walks `context.repository_def.asset_graph`.
2. Hashes the structural payload; skips push when unchanged from prior run (via `openmetadata_export/payload_hash` — err, `alation_export/payload_hash` — run tag). Toggle with `only_export_on_change`.
3. Transforms to Alation format (`dataflow_objects` + `paths`).
4. POSTs to `{catalog_url}/integration/v2/lineage/` with the API token in the `TOKEN` header.
5. Tags the run with the payload hash for next-run change detection.
