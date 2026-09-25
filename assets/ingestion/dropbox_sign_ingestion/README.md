# Dropbox Sign Ingestion

Ingest Dropbox Sign (formerly HelloSign) e-signature data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Dropbox Sign API key, used as the HTTP Basic auth username (password is blank). |
| `resources` | optional | Comma-separated list of resources to extract: signature_requests, templates. Default: `signature_requests,templates` |

## Example
```yaml
type: dagster_component_templates.DropboxSignIngestionComponent
attributes:
  asset_name: dropbox_sign_ingestion
  api_key: "${DROPBOX_SIGN_API_KEY}"
  resources: "signature_requests,templates"
```
