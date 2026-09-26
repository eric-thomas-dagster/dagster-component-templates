# PayPal Ingestion

Ingest PayPal transaction and invoice data (REST API, not Braintree) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** PayPal's transactions endpoint REQUIRES start_date/end_date and caps the window at roughly 31 days -- if you use a partition_type wider than daily/weekly, PayPal will likely reject the request; this connector does not clamp the window automatically. The exact data_selector key (transaction_details) is medium confidence. PayPal's Orders and Payouts APIs have no list endpoints at all (get-by-id only), so they are not offered as resources here.


> **Partition honesty note:** the `transactions` resource is genuinely bound to the partition window via `start_date`/`end_date` -- PayPal caps this window at roughly 31 days, so avoid `partition_type=monthly` or wider. `invoices` is snapshot-style and is **not** bound.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | PayPal REST app client ID. |
| `client_secret` | required | PayPal REST app client secret. |
| `resources` | optional | Comma-separated list of resources to extract: transactions (requires a bounded date range, max ~31 days), invoices. Default: `transactions` |

## Example
```yaml
type: dagster_component_templates.PayPalIngestionComponent
attributes:
  asset_name: paypal_ingestion
  client_id: "${PAYPAL_CLIENT_ID}"
  client_secret: "${PAYPAL_CLIENT_SECRET}"
  resources: "transactions"
```
