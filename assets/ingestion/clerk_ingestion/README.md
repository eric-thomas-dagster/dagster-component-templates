# Clerk Ingestion

Ingest Clerk user and organization data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `secret_key` | required | Clerk secret key (starts with sk_). |
| `resources` | optional | Comma-separated list of resources to extract: users, organizations, sessions. Default: `users,organizations` |

## Example
```yaml
type: dagster_component_templates.ClerkIngestionComponent
attributes:
  asset_name: clerk_ingestion
  secret_key: "${CLERK_SECRET_KEY}"
  resources: "users,organizations"
```
