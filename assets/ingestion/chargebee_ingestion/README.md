# Chargebee Ingestion

Ingest Chargebee subscription billing data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** the `invoices` and `transactions` resources are genuinely bound to the partition window via `created_at[after]`/`created_at[before]` params. `customers`, `subscriptions`, and `credit_notes` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `site` | required | Chargebee site name (for mysite.chargebee.com). |
| `api_key` | required | Chargebee API key, used as the HTTP Basic auth username (password is blank). |
| `resources` | optional | Comma-separated list of resources to extract: customers, subscriptions, invoices, transactions, credit_notes. Default: `customers,subscriptions,invoices` |

## Example
```yaml
type: dagster_component_templates.ChargebeeIngestionComponent
attributes:
  asset_name: chargebee_ingestion
  site: "mysite"
  api_key: "${CHARGEBEE_API_KEY}"
  resources: "customers,subscriptions,invoices"
```
