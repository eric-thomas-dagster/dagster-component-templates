# NetSuite Ingestion

Ingest Oracle NetSuite (SuiteTalk REST) data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** NetSuite's SuiteTalk REST API is authenticated via OAuth1.0a with static tokens (no interactive OAuth flow needed), but dltHub's documentation for NetSuite is thin. This connector passes a requests_oauthlib.OAuth1 instance directly as the client auth object, which relies on dlt's REST client accepting any requests-compatible auth callable -- this session did not independently confirm that against dlt's own config schema, and the in-process smoke test for this connector only proves the Python object construction doesn't crash, not that dlt's real REST client accepts it. Verify against a live NetSuite sandbox before production use, and check NetSuite's actual pagination behavior (unconfirmed here).


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `account_id` | required | NetSuite account ID (used in the tenant-specific SuiteTalk hostname). |
| `consumer_key` | required | OAuth1.0a consumer key from a NetSuite Integration record. |
| `consumer_secret` | required | OAuth1.0a consumer secret. |
| `token_id` | required | OAuth1.0a access token ID. |
| `token_secret` | required | OAuth1.0a access token secret. |
| `resources` | optional | Comma-separated list of resources to extract: customer, salesorder, invoice, vendor. Default: `customer,invoice` |

## Example
```yaml
type: dagster_component_templates.NetSuiteIngestionComponent
attributes:
  asset_name: netsuite_ingestion
  account_id: "1234567"
  consumer_key: "${NETSUITE_CONSUMER_KEY}"
  consumer_secret: "${NETSUITE_CONSUMER_SECRET}"
  token_id: "${NETSUITE_TOKEN_ID}"
  token_secret: "${NETSUITE_TOKEN_SECRET}"
  resources: "customer,invoice"
```
