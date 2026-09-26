# Amplitude Ingestion

Ingest Amplitude event definitions and cohort metadata using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** No confirmed 'list projects' endpoint exists in Amplitude's REST API -- project management appears to happen outside it, so this connector is scoped to event/cohort metadata for the single project identified by api_key/secret_key. The cohorts data_selector is medium confidence; Amplitude's real analytical power (event data itself, funnels, retention) is exposed via analysis endpoints, not a bulk list resource, and is out of scope here.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Amplitude project API key, used as the HTTP Basic auth username. |
| `secret_key` | required | Amplitude project secret key, used as the HTTP Basic auth password. |
| `eu_residency` | optional | Set true if your Amplitude project uses EU data residency. |
| `resources` | optional | Comma-separated list of resources to extract: events_list, cohorts. Default: `events_list` |

## Example
```yaml
type: dagster_component_templates.AmplitudeIngestionComponent
attributes:
  asset_name: amplitude_ingestion
  api_key: "${AMPLITUDE_API_KEY}"
  secret_key: "${AMPLITUDE_SECRET_KEY}"
  resources: "events_list"
```
