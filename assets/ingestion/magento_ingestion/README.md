# Magento / Adobe Commerce Ingestion

Ingest Magento/Adobe Commerce product, order, and customer data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** **Lower confidence.** Magento Open Source (self-hosted) and Adobe Commerce Cloud differ in hosting model, available modules, and integration setup -- this connector assumes a standard `/rest/V1` REST surface and a pre-obtained admin token, but only Magento's stockItems endpoint and 2FA flow were independently confirmed this session; the products/orders/customers paths and searchCriteria pagination shape come from general knowledge. Verify against your actual instance before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `instance_url` | required | Your Magento/Adobe Commerce instance base URL (self-hosted -- no fixed SaaS domain). |
| `admin_token` | required | Pre-generated Magento admin/integration access token (Admin > System > Integrations, or POST /V1/integration/admin/token). |
| `resources` | optional | Comma-separated list of resources to extract: products, orders, customers. Default: `products,orders` |

## Example
```yaml
type: dagster_component_templates.MagentoIngestionComponent
attributes:
  asset_name: magento_ingestion
  instance_url: "https://mystore.example.com"
  admin_token: "${MAGENTO_ADMIN_TOKEN}"
  resources: "products,orders"
```
