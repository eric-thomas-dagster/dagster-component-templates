# Xero Ingestion

Ingest Xero accounting data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Xero requires OAuth2 (refresh_token grant) plus a dynamic Xero-tenant-id header resolved from the /connections endpoint on every run -- there is no static API key. This connector uses the first tenant connection returned by /connections; if the refresh_token is authorized for multiple organizations, extend component.py to select a specific tenant. Exact list page size was not independently verified, and none of these resources are bound to the partition window (Xero's date-range filter syntax for the 'where' clause was not confirmed with enough confidence to hardcode) -- partition_type here is schedule-only.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Xero app client ID (used for the OAuth2 refresh-token exchange). |
| `client_secret` | required | Xero app client secret. |
| `refresh_token` | required | OAuth2 refresh token obtained once via Xero's OAuth consent flow (PKCE). |
| `resources` | optional | Comma-separated list of resources to extract: Invoices, Contacts, Accounts, BankTransactions. Default: `Invoices,Contacts` |

## Example
```yaml
type: dagster_component_templates.XeroIngestionComponent
attributes:
  asset_name: xero_ingestion
  client_id: "${XERO_CLIENT_ID}"
  client_secret: "${XERO_CLIENT_SECRET}"
  refresh_token: "${XERO_REFRESH_TOKEN}"
  resources: "Invoices,Contacts"
```
