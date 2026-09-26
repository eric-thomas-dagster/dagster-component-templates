# Paddle Ingestion

Ingest Paddle (merchant-of-record billing) customer, subscription, and transaction data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Paddle's pagination is a full next-page URL in meta.pagination.next (with has_more), not a simple param -- not independently confirmed whether dlt's default paginator auto-follows a full-URL-in-body cursor style.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Paddle API key. |
| `sandbox` | optional | Set true to use Paddle's sandbox environment. |
| `resources` | optional | Comma-separated list of resources to extract: customers, subscriptions, transactions. Default: `customers,subscriptions` |

## Example
```yaml
type: dagster_component_templates.PaddleIngestionComponent
attributes:
  asset_name: paddle_ingestion
  api_key: "${PADDLE_API_KEY}"
  resources: "customers,subscriptions"
```
