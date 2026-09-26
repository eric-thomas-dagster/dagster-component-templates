# Toggl Track Ingestion

Ingest Toggl Track time entries, projects, and clients using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** the `time_entries` resource is genuinely bound to the partition window via `since`/`before` params. `projects` and `clients` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | Toggl Track API token, used as the HTTP Basic auth username (password is the literal string 'api_token'). |
| `workspace_id` | optional | Toggl workspace ID. Required for projects/clients resources; not needed for time_entries. |
| `resources` | optional | Comma-separated list of resources to extract: time_entries (account-level); projects, clients (require workspace_id). Default: `time_entries` |

## Example
```yaml
type: dagster_component_templates.TogglTrackIngestionComponent
attributes:
  asset_name: toggl_track_ingestion
  api_token: "${TOGGL_API_TOKEN}"
  resources: "time_entries"
```
