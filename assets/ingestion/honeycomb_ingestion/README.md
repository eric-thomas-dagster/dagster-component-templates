# Honeycomb Ingestion

Ingest Honeycomb dataset metadata using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Honeycomb's dataset-level resources (triggers, markers, columns) require a dataset slug path param and were not implemented here to keep scope to the account-level datasets list; this connector only covers the datasets resource itself. Not independently confirmed live this session.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Honeycomb API key. |
| `eu_instance` | optional | Set true if your Honeycomb environment is hosted in the EU (api.eu1.honeycomb.io). |
| `resources` | optional | Comma-separated list of resources to extract: datasets. Default: `datasets` |

## Example
```yaml
type: dagster_component_templates.HoneycombIngestionComponent
attributes:
  asset_name: honeycomb_ingestion
  api_key: "${HONEYCOMB_API_KEY}"
  resources: "datasets"
```
