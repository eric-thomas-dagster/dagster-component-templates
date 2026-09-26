# `GoogleSheetsRowUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **Google Sheets worksheet** via **read-all, match locally, write** — Google Sheets has no upsert concept at the API level. This sink reads the entire worksheet, matches existing rows against `key_column` locally, updates changed rows in place, and appends new ones.

> **The sink owns the worksheet's layout.** The header row is rewritten to match `fields_map`'s values, in order, if it doesn't already match exactly — which clears any pre-existing data laid out differently. Point this at a dedicated tab, not a sheet a human is manually editing alongside it.

## When to use

- Sync computed data (scores, flags, statuses) from a warehouse INTO a Google Sheet so non-technical stakeholders can view/filter/sort it without needing warehouse access.

## Prerequisites

1. **Service account with edit access** to the target spreadsheet — share the sheet with the service account's email (found in the credentials JSON) from Google Sheets' own Share dialog.
2. **`key_column` must be present in `fields_map`** — it's how existing rows are recognized across runs.

## Pairs with

- **`google_sheets_resource`** — service account auth + gspread client (required).
- **`google_sheets_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `google_sheets_resource`) | Resource key registered by GoogleSheetsResourceComponent. |
| `spreadsheet_id` | required | Google Sheets spreadsheet ID (from the sheet URL). |
| `worksheet_name` | required | Worksheet (tab) name within the spreadsheet. |
| `key_column` | required | Upstream column used to match existing rows. Must be present in fields_map keys. |
| `fields_map` | required | Upstream column -> sheet column header, in display order. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.GoogleSheetsRowUpsertComponent
attributes:
  asset_name: sheets_customers_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: google_sheets_resource
  spreadsheet_id: "1AbCdEfGhIjKlMnOpQrStUvWxYz"
  worksheet_name: "Customers"
  key_column: email
  fields_map:
    email: Email
    name: Name
    health_score: "Health Score"
  group_name: reverse_etl
```
