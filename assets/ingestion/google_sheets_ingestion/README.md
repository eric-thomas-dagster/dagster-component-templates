# Google Sheets Ingestion Component


Ingest [Google Sheets](https://sheets.google.com) spreadsheet data using [dlt](https://dlthub.com)'s verified `google_sheets` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `credentials` | `Dict[str, Any]` | yes | Google service account credentials (JSON dict or string) |
| `spreadsheet_id` | `str` | yes | Spreadsheet ID (from the URL between `/d/` and `/edit`) |
| `sheet_names` | `List[str]` | yes | Sheet/tab names or A1 ranges to extract |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.GoogleSheetsIngestionComponent
attributes:
  asset_name: google_sheets_data
  # ... source config ...
  destination: bigquery          # or bigquery, postgres, filesystem, ...
  dataset_name: google_sheets_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.GoogleSheetsIngestionComponent
attributes:
  asset_name: gsheets_data
  credentials: { ... service account JSON ... }
  spreadsheet_id: "1abcdef..."
  sheet_names: [Customers, Orders]
  group_name: google_sheets
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Bigquery

```yaml
type: dagster_component_templates.GoogleSheetsIngestionComponent
attributes:
  asset_name: gsheets_data
  credentials: { ... service account JSON ... }
  spreadsheet_id: "1abcdef..."
  sheet_names: [Customers]
  destination: bigquery
  dataset_name: gsheets_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Sheet identifiers**: pass tab names or A1 ranges (`'Sheet1!A1:Z1000'`).
- **Sheet name normalization**: spaces and dashes are normalized to underscores when querying back from SQL destinations.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Google Sheets source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
