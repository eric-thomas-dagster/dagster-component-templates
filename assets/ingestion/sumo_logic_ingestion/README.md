# Sumo Logic Ingestion

Ingest Sumo Logic collector and source metadata using dlt's generic REST API source. Log/message search data requires an async job/poll pattern not implemented here -- this connector covers collector/source metadata only.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Log/message search data in Sumo Logic requires an async job pattern (POST /v1/search/jobs, poll for completion, then GET .../messages or .../records) -- fundamentally not a simple paginated list, and NOT implemented here. This connector is scoped to collector/source metadata only; if you need actual log data, extend this connector with the job-polling flow separately.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_id` | required | Sumo Logic access ID, used as the HTTP Basic auth username. |
| `access_key` | required | Sumo Logic access key, used as the HTTP Basic auth password. |
| `deployment` | optional | Sumo Logic deployment pod: 'us1' (default), 'us2', 'au', 'ca', 'de', 'eu', 'fed', 'in', or 'jp'. Must match your account. |
| `collector_id` | optional | Specific collector ID to list sources for (enables the sources resource). |
| `resources` | optional | Comma-separated list of resources to extract: collectors (account-level); sources (requires collector_id). Default: `collectors` |

## Example
```yaml
type: dagster_component_templates.SumoLogicIngestionComponent
attributes:
  asset_name: sumo_logic_ingestion
  access_id: "${SUMOLOGIC_ACCESS_ID}"
  access_key: "${SUMOLOGIC_ACCESS_KEY}"
  resources: "collectors"
```
