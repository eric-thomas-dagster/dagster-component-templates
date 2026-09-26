# Auth0 (Management API) Ingestion

Ingest Auth0 org data (users, roles, organizations) via the Management API using dlt's generic REST API source. Distinct from auth0_logs_ingestion, which covers audit/log-stream data only.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Auth0's default offset pagination (page/per_page) caps at 1000 total results for most endpoints -- for larger tenants, use checkpoint pagination (from/take) instead, which this connector does not implement. Not independently confirmed whether dlt's default paginator auto-detects Auth0's pagination style.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `tenant_domain` | required | Your Auth0 tenant domain. |
| `client_id` | required | Auth0 Machine-to-Machine application client ID (authorized for the Management API). |
| `client_secret` | required | Auth0 M2M application client secret. |
| `resources` | optional | Comma-separated list of resources to extract: users, roles, organizations. Default: `users,roles` |

## Example
```yaml
type: dagster_component_templates.Auth0ManagementIngestionComponent
attributes:
  asset_name: auth0_management_ingestion
  tenant_domain: "mycompany.us.auth0.com"
  client_id: "${AUTH0_CLIENT_ID}"
  client_secret: "${AUTH0_CLIENT_SECRET}"
  resources: "users,roles"
```
