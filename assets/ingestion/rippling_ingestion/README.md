# Rippling Ingestion

Ingest Rippling HR/workforce data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | Rippling API bearer token. Use ${RIPPLING_API_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: workers, users, departments, job_requisitions, compensation. Default: `workers,departments` |

## Example
```yaml
type: dagster_component_templates.RippleIngestionComponent
attributes:
  asset_name: rippling_ingestion
  api_token: "${RIPPLING_API_TOKEN}"
  resources: "workers,departments"
```
