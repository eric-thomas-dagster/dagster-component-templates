# BigCommerce Ingestion

Ingest BigCommerce product, customer, and category data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `store_hash` | required | BigCommerce store hash (from your store's API path). |
| `access_token` | required | BigCommerce API access token, sent as the X-Auth-Token header. |
| `resources` | optional | Comma-separated list of resources to extract: products, customers, categories. Default: `products,customers` |

## Example
```yaml
type: dagster_component_templates.BigCommerceIngestionComponent
attributes:
  asset_name: bigcommerce_ingestion
  store_hash: "abc123def"
  access_token: "${BIGCOMMERCE_ACCESS_TOKEN}"
  resources: "products,customers"
```
