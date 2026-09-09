# WebhookLineageExportJobComponent

Op-shaped job that **walks the live Dagster asset graph** on each run
and POSTs the raw lineage payload to any HTTP webhook (Slack, internal
endpoint, n8n, Zapier, custom automation). Optional Bearer auth.

Distinct from the more general [`http_webhook_job`](../http_webhook_job/)
(which posts a static / configurable body). This one specifically
composes the Dagster asset-graph lineage payload and pushes THAT —
matching the asset-shaped `lineage_to_webhook` sink.

## When to use this vs `lineage_to_webhook`

| Component | Shape | Use case |
|---|---|---|
| `lineage_graph_extractor` + `lineage_to_webhook` | **3-asset chain** | Lineage as a first-class Dagster asset. Automation-condition-driven pushes when upstream changes. |
| `WebhookLineageExportJobComponent` (this) | **single op-job** | Scheduled push; no asset overhead. Usually the better default for "push my Dagster asset graph to X endpoint nightly." |

## Payload shape

The POST body is the raw lineage payload:

```json
{
  "source_system": {"platform": "dagster", "deployment": "prod", "dagster_ui_url": "..."},
  "sync_metadata": {"synced_at": "...", "source": "dagster_asset_graph", "total_nodes": N, "total_edges": M},
  "nodes": [{"asset_key": [...], "asset_key_string": "...", "group": "...", "kinds": [...], "description": "...", "metadata": {...}}, ...],
  "edges": [{"upstream": "...", "downstream": "..."}, ...]
}
```

## YAML example

```yaml
type: dagster_component_templates.WebhookLineageExportJobComponent
attributes:
  job_name: push_dagster_lineage_to_webhook
  schedule: "0 3 * * *"
  default_status: RUNNING
  webhook_url: https://n8n.acme.com/webhook/dagster-lineage
  api_token_env: LINEAGE_WEBHOOK_TOKEN  # omit for unauthenticated
  only_export_on_change: true
  fail_on_webhook_error: true
```

## Required env vars

```bash
# Only if the webhook requires auth:
LINEAGE_WEBHOOK_TOKEN=...            # sent as Authorization: Bearer

# Optional — populates source_system fields:
DAGSTER_UI_URL=https://dagster.acme.com
DAGSTER_DEPLOYMENT=prod
```
