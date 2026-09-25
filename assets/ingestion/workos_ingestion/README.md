# WorkOS Ingestion

Ingest WorkOS SSO/directory-sync data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | WorkOS API key (starts with sk_). |
| `resources` | optional | Comma-separated list of resources to extract: connections, organizations, directory_users, directory_groups, audit_logs. Default: `connections,organizations` |

## Example
```yaml
type: dagster_component_templates.WorkOSIngestionComponent
attributes:
  asset_name: workos_ingestion
  api_key: "${WORKOS_API_KEY}"
  resources: "connections,organizations"
```
