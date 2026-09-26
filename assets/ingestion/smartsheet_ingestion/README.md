# Smartsheet Ingestion

Ingest Smartsheet sheet metadata, users, reports, and (optionally) one sheet's row data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** The sheet_rows resource returns Smartsheet's raw row objects, where each cell is keyed by columnId rather than column name (Smartsheet's API does not flatten this) -- you will likely want a downstream transform to map columnId -> column title using the sheets resource's column metadata before this is analysis-ready.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Smartsheet API access token (personal or from an OAuth2 app). |
| `sheet_id` | optional | Specific sheet ID to pull row data from (enables the sheet_rows resource). Find it via the sheets resource or the sheet's own URL. |
| `resources` | optional | Comma-separated list of resources to extract: sheets (metadata list), users, reports, sheet_rows (requires sheet_id). Default: `sheets,users` |

## Example
```yaml
type: dagster_component_templates.SmartsheetIngestionComponent
attributes:
  asset_name: smartsheet_ingestion
  access_token: "${SMARTSHEET_ACCESS_TOKEN}"
  resources: "sheets,users"
```
