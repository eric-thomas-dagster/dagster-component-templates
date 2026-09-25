# Recurly Ingestion

Ingest Recurly subscription billing data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** the `invoices` and `transactions` resources are genuinely bound to the partition window via `begin_time`/`end_time` params. `accounts`, `subscriptions`, and `coupons` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Recurly private API key, used as the HTTP Basic auth username (password is blank). |
| `resources` | optional | Comma-separated list of resources to extract: accounts, subscriptions, invoices, transactions, coupons. Default: `accounts,subscriptions,invoices` |

## Example
```yaml
type: dagster_component_templates.RecurlyIngestionComponent
attributes:
  asset_name: recurly_ingestion
  api_key: "${RECURLY_API_KEY}"
  resources: "accounts,subscriptions,invoices"
```
