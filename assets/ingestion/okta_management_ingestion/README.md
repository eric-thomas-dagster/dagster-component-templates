# Okta (Management API) Ingestion

Ingest Okta org data (users, groups, apps) via the Management API using dlt's generic REST API source. Distinct from okta_system_log_ingestion, which covers audit/system logs only.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Pagination is cursor-based via the HTTP `Link` response header (rel="next"), not a JSON body field -- this session did not independently confirm dlt's default rest_api paginator auto-detects Link-header pagination; verify against an org with more than one page of users before relying on complete pulls.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `org_url` | required | Your Okta org URL. |
| `api_token` | required | Okta API token (SSWS), created under Security > API > Tokens. |
| `resources` | optional | Comma-separated list of resources to extract: users, groups, apps. Default: `users,groups` |

## Example
```yaml
type: dagster_component_templates.OktaManagementIngestionComponent
attributes:
  asset_name: okta_management_ingestion
  org_url: "https://mycompany.okta.com"
  api_token: "${OKTA_API_TOKEN}"
  resources: "users,groups"
```
