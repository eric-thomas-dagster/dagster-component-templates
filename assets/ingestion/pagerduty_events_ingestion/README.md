# PagerDuty Ingestion

Ingest PagerDuty incidents/services/schedules using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- PagerDuty has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | PagerDuty REST API token. Use ${PAGERDUTY_API_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: incidents, services, schedules, escalation_policies. Default: `incidents,services,schedules` |

## Example

```yaml
type: dagster_component_templates.PagerDutyIngestionComponent
attributes:
  asset_name: pagerduty_events_ingestion
  api_token: "${PAGERDUTY_API_TOKEN}"
  resources: "incidents,services,schedules"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/pagerduty_events`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
