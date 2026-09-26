# Dropbox Ingestion

Ingest Dropbox file/folder listings via a hand-written cursor-paginated fetch.

> **Note:** Dropbox's file-listing API (`files/list_folder`) is POST + JSON-body, not GET + query-params -- an awkward, unverified fit for dlt's generic `rest_api_source`, which assumes REST path/query-param semantics. Also, Dropbox deprecated long-lived static access tokens; only a refresh_token OAuth2 flow is supported now. Rather than force a bad fit, this connector is hand-written with a plain `@dlt.resource` generator that POSTs directly via `requests` and walks Dropbox's own cursor/`has_more` continuation protocol -- the same reasoning already applied to Linear and monday.com (GraphQL-only APIs) elsewhere in this repo.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Dropbox app client ID. |
| `client_secret` | required | Dropbox app client secret. |
| `refresh_token` | required | OAuth2 refresh token obtained once via Dropbox's OAuth consent flow (token_access_type=offline). |
| `folder_path` | optional (default `""`) | Dropbox folder path to list. Empty lists the root. |
| `recursive` | optional (default `true`) | List all nested subfolders too. |
| `resources` | optional | Comma-separated list of resources to extract: files. Default: `files` |

## Partitions

Not bound to the partition window -- every run re-lists the same full folder tree regardless of partition. Declaring `partition_type` controls Dagster's materialization/backfill schedule only.

## Example
```yaml
type: dagster_component_templates.DropboxIngestionComponent
attributes:
  asset_name: dropbox_ingestion
  client_id: "${DROPBOX_CLIENT_ID}"
  client_secret: "${DROPBOX_CLIENT_SECRET}"
  refresh_token: "${DROPBOX_REFRESH_TOKEN}"
  folder_path: ""
```
