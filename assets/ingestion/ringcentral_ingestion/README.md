# RingCentral Ingestion

Ingest RingCentral call logs, messages, and extension data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** RingCentral's JWT-bearer flow requires an admin to create a 'Custom Rest API App' with JWT enabled and issue a JWT credential string (this is a fixed, non-expiring credential distinct from a minted access token) -- this connector exchanges it for a fresh access token on every run. Pagination (navigation.nextPage) is medium confidence.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | RingCentral app client ID. |
| `client_secret` | required | RingCentral app client secret. |
| `jwt_token` | required | Pre-issued RingCentral JWT credential (from an admin-created Custom Rest API app with JWT flow enabled). |
| `resources` | optional | Comma-separated list of resources to extract: extensions, call_logs, messages, teams. Default: `call_logs,extensions` |

## Example
```yaml
type: dagster_component_templates.RingCentralIngestionComponent
attributes:
  asset_name: ringcentral_ingestion
  client_id: "${RINGCENTRAL_CLIENT_ID}"
  client_secret: "${RINGCENTRAL_CLIENT_SECRET}"
  jwt_token: "${RINGCENTRAL_JWT_TOKEN}"
  resources: "call_logs,extensions"
```
