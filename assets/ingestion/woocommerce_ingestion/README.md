# WooCommerce Ingestion

Ingest WooCommerce store data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** the `orders` resource is genuinely bound to the partition window via `after`/`before` params. `products`, `customers`, and `coupons` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `store_url` | required | Base URL of your WooCommerce store (the /wp-json/wc/v3 path is appended automatically). |
| `consumer_key` | required | WooCommerce REST API consumer key, used as the HTTP Basic auth username. |
| `consumer_secret` | required | WooCommerce REST API consumer secret, used as the HTTP Basic auth password. |
| `resources` | optional | Comma-separated list of resources to extract: products, orders, customers, coupons. Default: `products,orders` |

## Example
```yaml
type: dagster_component_templates.WooCommerceIngestionComponent
attributes:
  asset_name: woocommerce_ingestion
  store_url: "https://mystore.com"
  consumer_key: "${WOOCOMMERCE_CONSUMER_KEY}"
  consumer_secret: "${WOOCOMMERCE_CONSUMER_SECRET}"
  resources: "products,orders"
```
