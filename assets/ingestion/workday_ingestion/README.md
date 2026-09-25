# Workday Ingestion

Ingest Workday HCM worker data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** **Low confidence.** Workday's REST surface varies significantly by module (HCM vs Financials) and by tenant/datacenter -- there is no single fixed base_url or token_url, so both must be supplied per-tenant (find them in your Workday tenant's API client registration). Only the `workers` resource is wired up here (path='workers', data_selector='data'), reconstructed from general knowledge rather than a confirmed live fetch. Much of Workday integration is still SOAP or Report-as-a-Service (RaaS) based rather than pure REST -- validate this connector against your own tenant's actual API docs before relying on it, and extend the resource map in component.py for additional endpoints.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `base_url` | required | Full Workday REST API base URL for your tenant/datacenter (found in your Workday API client registration). |
| `token_url` | required | Full OAuth2 token endpoint URL for your tenant. |
| `client_id` | required | Workday Integration System User (ISU) OAuth2 client ID. |
| `client_secret` | required | Workday OAuth2 client secret. |
| `refresh_token` | required | Workday OAuth2 refresh token. |
| `resources` | optional | Comma-separated list of resources to extract: workers (only resource currently wired up -- see confidence note). Default: `workers` |

## Example
```yaml
type: dagster_component_templates.WorkdayIngestionComponent
attributes:
  asset_name: workday_ingestion
  base_url: "https://wd2-impl-services1.workday.com/ccx/api/v1/acme_gms1"
  token_url: "https://wd2-impl-services1.workday.com/ccx/oauth2/acme_gms1/token"
  client_id: "${WORKDAY_CLIENT_ID}"
  client_secret: "${WORKDAY_CLIENT_SECRET}"
  refresh_token: "${WORKDAY_REFRESH_TOKEN}"
  resources: "workers"
```
