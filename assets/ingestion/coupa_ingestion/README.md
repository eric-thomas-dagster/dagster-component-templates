# Coupa Ingestion

Ingest Coupa procurement data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Coupa's OAuth2 client_credentials flow and tenant-subdomain base_url pattern are well-established, but exact resource paths/casing (suppliers, purchase_orders, invoices, users, expense_reports) were not independently confirmed against live Coupa docs -- verify against your instance's own API browser before production use. Coupa access tokens reportedly expire after ~20 hours; this connector re-authenticates on every run.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `instance` | required | Coupa instance subdomain (for mycompany.coupahost.com). |
| `client_id` | required | Coupa OAuth2 client_credentials client ID (integration user). |
| `client_secret` | required | Coupa OAuth2 client_credentials client secret. |
| `resources` | optional | Comma-separated list of resources to extract: suppliers, purchase_orders, invoices, users, expense_reports. Default: `suppliers,purchase_orders` |

## Example
```yaml
type: dagster_component_templates.CoupaIngestionComponent
attributes:
  asset_name: coupa_ingestion
  instance: "mycompany"
  client_id: "${COUPA_CLIENT_ID}"
  client_secret: "${COUPA_CLIENT_SECRET}"
  resources: "suppliers,purchase_orders"
```
