# Gusto Ingestion

Ingest Gusto HR/payroll data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Gusto has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `company_id` | required | Gusto company UUID. |
| `access_token` | required | Gusto OAuth2 access token. Use ${GUSTO_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: employees, payrolls, time_off_requests. Default: `employees,payrolls,time_off_requests` |

## Example

```yaml
type: dagster_component_templates.GustoIngestionComponent
attributes:
  asset_name: gusto_ingestion
  company_id: "${GUSTO_COMPANY_ID}"
  access_token: "${GUSTO_ACCESS_TOKEN}"
  resources: "employees,payrolls,time_off_requests"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/gusto`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
