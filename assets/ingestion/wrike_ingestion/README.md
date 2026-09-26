# Wrike Ingestion

Ingest Wrike task, folder, and contact data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** EU/US2 Wrike accounts may use a different regional base_url than the default www.wrike.com -- check your account's actual API host if requests fail with an auth or redirect error.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Wrike permanent access token (Apps & Integrations > API). |
| `resources` | optional | Comma-separated list of resources to extract: tasks, folders, contacts. Default: `tasks,folders` |

## Example
```yaml
type: dagster_component_templates.WrikeIngestionComponent
attributes:
  asset_name: wrike_ingestion
  access_token: "${WRIKE_ACCESS_TOKEN}"
  resources: "tasks,folders"
```
