# Fullstory Ingestion

Ingest Fullstory user, segment, and settings data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Fullstory's exact list-response shape (bare array vs wrapped) was not independently confirmed against live docs this session -- verify data_selector against a real response before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Fullstory API key, used as the HTTP Basic auth username (password is blank). |
| `resources` | optional | Comma-separated list of resources to extract: users, segments. Default: `users,segments` |

## Example
```yaml
type: dagster_component_templates.FullstoryIngestionComponent
attributes:
  asset_name: fullstory_ingestion
  api_key: "${FULLSTORY_API_KEY}"
  resources: "users,segments"
```
