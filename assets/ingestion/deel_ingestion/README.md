# Deel Ingestion

Ingest Deel global payroll/contractor data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | Deel API bearer token. Use ${DEEL_API_TOKEN} for env vars. |
| `client_id` | optional | Deel OAuth2 client ID (sent as x-client-id header; only needed for OAuth2 apps, not personal API tokens). |
| `resources` | optional | Comma-separated list of resources to extract: people, contracts, legal_entities, invoices. Default: `people,contracts` |

## Example
```yaml
type: dagster_component_templates.DeelIngestionComponent
attributes:
  asset_name: deel_ingestion
  api_token: "${DEEL_API_TOKEN}"
  resources: "people,contracts"
```
