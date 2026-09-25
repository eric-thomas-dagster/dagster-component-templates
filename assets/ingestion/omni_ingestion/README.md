# Omni Ingestion

Ingest Omni BI account metadata (dashboards, models, queries) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Not found on dltHub's reference catalog (404) -- this is account-level metadata ingestion (dashboards/models/queries lists), distinct from any dashboard-embedding integration. Base_url/auth/resource paths are a best-effort placeholder shape, not verified against Omni's own API docs; confirm the real base_url and endpoint names before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `organization` | required | Omni organization subdomain (from https://{organization}.omniapp.co). |
| `api_key` | required | Omni API key. Use ${OMNI_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: dashboards, models, queries. Default: `dashboards,models` |

## Example
```yaml
type: dagster_component_templates.OmniIngestionComponent
attributes:
  asset_name: omni_ingestion
  organization: "myorg"
  api_key: "${OMNI_API_KEY}"
  resources: "dashboards,models"
```
