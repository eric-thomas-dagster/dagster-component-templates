# Confluence Ingestion

Ingest Confluence Cloud pages and spaces using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `site_domain` | required | Your Atlassian site subdomain (for yoursite.atlassian.net). |
| `email` | required | Atlassian account email for HTTP Basic auth. |
| `api_token` | required | Atlassian API token, used as the Basic auth password. |
| `space_id` | optional | Optional space ID to scope pages/blogposts to a single space. |
| `resources` | optional | Comma-separated list of resources to extract: pages, spaces, blogposts, attachments. Default: `pages,spaces` |

## Example
```yaml
type: dagster_component_templates.ConfluenceIngestionComponent
attributes:
  asset_name: confluence_ingestion
  site_domain: "yoursite"
  email: "${CONFLUENCE_EMAIL}"
  api_token: "${CONFLUENCE_API_TOKEN}"
  resources: "pages,spaces"
```
