# Opsgenie Ingestion

Ingest Opsgenie alerting/on-call data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Opsgenie has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Opsgenie API key. Use ${OPSGENIE_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: alerts, schedules, teams, users. Default: `alerts,schedules,teams,users` |

## Example

```yaml
type: dagster_component_templates.OpsgenieIngestionComponent
attributes:
  asset_name: opsgenie_ingestion
  api_key: "${OPSGENIE_API_KEY}"
  resources: "alerts,schedules,teams,users"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/opsgenie`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
