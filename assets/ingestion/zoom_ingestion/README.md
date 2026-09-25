# Zoom Ingestion

Ingest Zoom meetings/webinars/users data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Zoom OAuth2 access token (Server-to-Server OAuth app). Use ${ZOOM_ACCESS_TOKEN} for env vars. |
| `user_id` | optional | Zoom user ID to scope meetings/webinars to. Defaults to 'me' (the token's own user). |
| `resources` | optional | Comma-separated list of resources to extract: users, meetings, upcoming_meetings, webinars. Default: `users,meetings,webinars` |

## Example
```yaml
type: dagster_component_templates.ZoomIngestionComponent
attributes:
  asset_name: zoom_ingestion
  access_token: "${ZOOM_ACCESS_TOKEN}"
  user_id: "me"
  resources: "users,meetings,webinars"
```
