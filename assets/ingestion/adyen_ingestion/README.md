# Adyen Ingestion

Ingest Adyen merchant and store data via the Management API using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Adyen's live API host includes a company-specific prefix found in your Customer Area (not a fixed domain) -- get this exactly right or requests will fail to resolve. The 'data' data_selector follows Adyen's general API convention but was not independently confirmed for the Management API specifically this session.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Adyen API key, sent as the X-API-Key header. |
| `is_live` | optional | Set true for the live environment. Defaults to the test environment. |
| `live_url_prefix` | optional | Your company-specific live URL prefix (required if is_live=true -- found in the Adyen Customer Area). |
| `merchant_id` | optional | Merchant account ID. Required for the stores resource. |
| `resources` | optional | Comma-separated list of resources to extract: merchants (account-level); stores (requires merchant_id). Default: `merchants` |

## Example
```yaml
type: dagster_component_templates.AdyenIngestionComponent
attributes:
  asset_name: adyen_ingestion
  api_key: "${ADYEN_API_KEY}"
  resources: "merchants"
```
