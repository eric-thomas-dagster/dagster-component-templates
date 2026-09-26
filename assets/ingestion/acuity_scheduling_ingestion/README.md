# Acuity Scheduling Ingestion

Ingest Acuity Scheduling appointment, client, and calendar data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** the `appointments` resource is genuinely bound to the partition window via `minDate`/`maxDate`. `clients`, `calendars`, and `appointment_types` are snapshot-style and are **not** bound.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `user_id` | required | Acuity numeric user ID, used as the HTTP Basic auth username. |
| `api_key` | required | Acuity API key, used as the HTTP Basic auth password. |
| `resources` | optional | Comma-separated list of resources to extract: appointments, clients, calendars, appointment_types. Default: `appointments,clients` |

## Example
```yaml
type: dagster_component_templates.AcuitySchedulingIngestionComponent
attributes:
  asset_name: acuity_scheduling_ingestion
  user_id: "${ACUITY_USER_ID}"
  api_key: "${ACUITY_API_KEY}"
  resources: "appointments,clients"
```
