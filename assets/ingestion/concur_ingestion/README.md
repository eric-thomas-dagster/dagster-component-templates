# SAP Concur Ingestion

Ingest SAP Concur travel/expense data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** dltHub's reference page for Concur only confirmed base_url, bearer auth, and the resource names themselves (users, vendors, expense) -- exact response shapes/data_selectors and pagination weren't documented there. Verify against SAP Concur's own API docs before production use.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | SAP Concur OAuth2 access token. Use ${CONCUR_ACCESS_TOKEN} for env vars. |
| `datacenter_url` | optional | Region-specific Concur API base (e.g. us/eu/cn datacenter). Defaults to the US datacenter. |
| `resources` | optional | Comma-separated list of resources to extract: users, vendors, expense. Default: `users,expense` |

## Example
```yaml
type: dagster_component_templates.ConcurIngestionComponent
attributes:
  asset_name: concur_ingestion
  access_token: "${CONCUR_ACCESS_TOKEN}"
  datacenter_url: "https://us.api.concursolutions.com"
  resources: "users,expense"
```
