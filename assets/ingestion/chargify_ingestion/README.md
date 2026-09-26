# Chargify (Maxio Advanced Billing) Ingestion

Ingest Chargify/Maxio Advanced Billing subscription, customer, and invoice data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Some Chargify list items are individually wrapped (e.g. each array element shaped like {"subscription": {...}} rather than a flat object) -- this connector does not unwrap that per-item nesting; check your actual response shape and add a downstream transform if needed. Pagination is page/per_page (default 20, max 200, 1-indexed) -- this connector does not loop pages automatically beyond what dlt's default paginator detects.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `subdomain` | required | Chargify subdomain (for mysite.chargify.com). |
| `api_key` | required | Chargify API key, used as the HTTP Basic auth username (password is the literal string 'x'). |
| `resources` | optional | Comma-separated list of resources to extract: subscriptions, customers, invoices, products. Default: `subscriptions,customers` |

## Example
```yaml
type: dagster_component_templates.ChargifyIngestionComponent
attributes:
  asset_name: chargify_ingestion
  subdomain: "mysite"
  api_key: "${CHARGIFY_API_KEY}"
  resources: "subscriptions,customers"
```
