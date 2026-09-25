# QuickBooks Online Ingestion

Ingest QuickBooks Online accounting data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** QuickBooks Online has no static API key; this connector performs an OAuth2 refresh-token exchange on every run using client_id/client_secret/refresh_token (the initial refresh_token must be obtained once via QuickBooks' OAuth consent flow). QuickBooks paginates via STARTPOSITION/MAXRESULTS embedded in the query string itself rather than a standard cursor -- this connector currently fetches only the first 1000 rows per resource; extend the query loop in component.py for larger datasets. The Invoice resource is genuinely bound to the partition window via a TxnDate range in the query; Customer/Item/Account are snapshot-style and are not.


> **Partition honesty note:** the `Invoice` resource is genuinely bound to the partition window via a `TxnDate` range in the query. `Customer`, `Item`, and `Account` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | QuickBooks app client ID (used for the OAuth2 refresh-token exchange). |
| `client_secret` | required | QuickBooks app client secret. |
| `refresh_token` | required | OAuth2 refresh token obtained once via QuickBooks' OAuth consent flow. Expires after ~100 days of inactivity. |
| `realm_id` | required | QuickBooks company (realm) ID. |
| `resources` | optional | Comma-separated list of resources to extract: Customer, Invoice, Item, Account. Default: `Customer,Invoice` |

## Example
```yaml
type: dagster_component_templates.QuickBooksIngestionComponent
attributes:
  asset_name: quickbooks_ingestion
  client_id: "${QUICKBOOKS_CLIENT_ID}"
  client_secret: "${QUICKBOOKS_CLIENT_SECRET}"
  refresh_token: "${QUICKBOOKS_REFRESH_TOKEN}"
  realm_id: "${QUICKBOOKS_REALM_ID}"
  resources: "Customer,Invoice"
```
