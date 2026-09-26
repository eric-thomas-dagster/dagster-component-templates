# Constant Contact Ingestion

Ingest Constant Contact contact and list data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Constant Contact's API is OAuth2-only -- there is no static API-key path, so the initial refresh_token must be obtained once via Constant Contact's OAuth consent flow before this connector can be configured. Pagination is cursor-based via `_links.next.href` in the response body -- not independently confirmed to be auto-handled by dlt's default paginator.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Constant Contact app client ID (used for the OAuth2 refresh-token exchange). |
| `client_secret` | required | Constant Contact app client secret. |
| `refresh_token` | required | OAuth2 refresh token obtained once via Constant Contact's OAuth consent flow -- no static API-key-only path exists for this API. |
| `resources` | optional | Comma-separated list of resources to extract: contacts, contact_lists, contact_custom_fields. Default: `contacts,contact_lists` |

## Example
```yaml
type: dagster_component_templates.ConstantContactIngestionComponent
attributes:
  asset_name: constant_contact_ingestion
  client_id: "${CONSTANT_CONTACT_CLIENT_ID}"
  client_secret: "${CONSTANT_CONTACT_CLIENT_SECRET}"
  refresh_token: "${CONSTANT_CONTACT_REFRESH_TOKEN}"
  resources: "contacts,contact_lists"
```
