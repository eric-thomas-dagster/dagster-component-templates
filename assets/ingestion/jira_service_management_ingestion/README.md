# Jira Service Management Ingestion

Ingest Jira Service Management service desk, request, and organization data using dlt's generic REST API source. Distinct product surface from core Jira.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `site_domain` | required | Your Atlassian site subdomain (for mysite.atlassian.net). |
| `email` | required | Atlassian account email for HTTP Basic auth. |
| `api_token` | required | Atlassian API token, used as the Basic auth password. |
| `resources` | optional | Comma-separated list of resources to extract: servicedesks, requests, organizations. Default: `servicedesks,requests` |

## Example
```yaml
type: dagster_component_templates.JiraServiceManagementIngestionComponent
attributes:
  asset_name: jira_service_management_ingestion
  site_domain: "mysite"
  email: "${JSM_EMAIL}"
  api_token: "${JSM_API_TOKEN}"
  resources: "servicedesks,requests"
```
