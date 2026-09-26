# Pendo Ingestion

Ingest Pendo product analytics metadata (features, guides, accounts) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Pendo's main analytics access (visitor/account activity data) goes through a separate POST /api/v1/aggregation pipeline endpoint, not a simple GET list -- this connector only covers the metadata list endpoints (feature/guide/account definitions), not usage analytics. Verify against live docs before assuming this covers your Pendo use case.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `integration_key` | required | Pendo integration key. |
| `eu_tenant` | optional | Set true if your Pendo tenant is hosted in the EU (app.eu.pendo.io). |
| `resources` | optional | Comma-separated list of resources to extract: features, guides, accounts. Default: `features,guides` |

## Example
```yaml
type: dagster_component_templates.PendoIngestionComponent
attributes:
  asset_name: pendo_ingestion
  integration_key: "${PENDO_INTEGRATION_KEY}"
  resources: "features,guides"
```
