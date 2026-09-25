# Paylocity Ingestion

Ingest Paylocity payroll/HR data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Not found on dltHub's reference catalog (404) -- base_url, auth, and resource paths here are from general knowledge of Paylocity's public Web Services API, not a verified dltHub source page. Verify against Paylocity's own API docs before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `company_id` | required | Paylocity company ID. |
| `access_token` | required | Paylocity OAuth2 access token (client-credentials grant). Use ${PAYLOCITY_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: employees, earnings, deductions. Default: `employees` |

## Example
```yaml
type: dagster_component_templates.PaylocityIngestionComponent
attributes:
  asset_name: paylocity_ingestion
  company_id: "${PAYLOCITY_COMPANY_ID}"
  access_token: "${PAYLOCITY_ACCESS_TOKEN}"
  resources: "employees"
```
