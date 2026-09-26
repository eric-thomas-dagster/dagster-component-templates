# Zuora Ingestion

Ingest Zuora subscription billing data (accounts, subscriptions, invoices) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** The OAuth2 client_credentials flow and 1hr token expiry are confirmed, but the exact response envelope/array key for accounts/subscriptions/invoices (assumed here to match the resource name) was NOT independently confirmed against live docs -- verify against your Zuora tenant before production use. Pagination style is unconfirmed.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `base_url` | required | Your Zuora REST endpoint (region/environment-specific, e.g. rest.na.zuora.com, rest.eu.zuora.com, or a sandbox host). |
| `client_id` | required | Zuora OAuth client ID. |
| `client_secret` | required | Zuora OAuth client secret. |
| `resources` | optional | Comma-separated list of resources to extract: accounts, subscriptions, invoices. Default: `accounts,subscriptions` |

## Example
```yaml
type: dagster_component_templates.ZuoraIngestionComponent
attributes:
  asset_name: zuora_ingestion
  base_url: "https://rest.na.zuora.com"
  client_id: "${ZUORA_CLIENT_ID}"
  client_secret: "${ZUORA_CLIENT_SECRET}"
  resources: "accounts,subscriptions"
```
