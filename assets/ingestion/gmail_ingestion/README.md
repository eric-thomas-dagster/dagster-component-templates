# Gmail Ingestion

Ingest Gmail messages/labels metadata via the Gmail API using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Not found on dltHub's reference catalog (404) -- base_url and resource paths here are from Google's own public Gmail API reference (well-documented, high confidence on the endpoint shapes), not a dltHub source page. Note this only pulls metadata (headers/labels), not full message bodies, by default.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Google OAuth2 access token scoped for gmail.readonly. Use ${GMAIL_ACCESS_TOKEN} for env vars. |
| `query` | optional | Gmail search query to filter messages (e.g. 'after:2024/01/01'). Only used for the messages resource. |
| `resources` | optional | Comma-separated list of resources to extract: messages, labels, threads. Default: `messages,labels` |

## Example
```yaml
type: dagster_component_templates.GmailIngestionComponent
attributes:
  asset_name: gmail_ingestion
  access_token: "${GMAIL_ACCESS_TOKEN}"
  resources: "messages,labels"
```
