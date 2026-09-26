# Frontegg Ingestion

Ingest Frontegg (auth-as-a-service) user and tenant data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** **Lower confidence on resource paths.** The two-step vendor-auth flow (POST /auth/vendor with clientId/secret -> a short-lived vendor JWT) is solid, but the exact user/tenant resource paths and response envelope (items vs another wrapper) were NOT independently confirmed this session -- Frontegg's docs site returned 404s during research. Verify against live docs or a test call before production use. Default region is EU per Frontegg's own default; override region for US/CA/AU accounts.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Frontegg vendor client ID. |
| `api_key` | required | Frontegg vendor API key/secret. |
| `region` | optional | Frontegg region: 'us' (default), 'eu', 'ca', or 'au'. |
| `resources` | optional | Comma-separated list of resources to extract: users, tenants. Default: `users,tenants` |

## Example
```yaml
type: dagster_component_templates.FronteggIngestionComponent
attributes:
  asset_name: frontegg_ingestion
  client_id: "${FRONTEGG_CLIENT_ID}"
  api_key: "${FRONTEGG_API_KEY}"
  resources: "users,tenants"
```
