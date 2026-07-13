# GoogleSheetsWorkspaceComponent

Auto-emit one Dagster asset per **(spreadsheet, tab)** in a Google Drive folder. `StateBackedComponent` — discovery cached to disk. Materializing reads the tab's values via the Sheets API and returns a DataFrame with the header row as column names.

## Example

```yaml
type: dagster_community_components.GoogleSheetsWorkspaceComponent
attributes:
  credentials_json_env_var: GS_SERVICE_ACCOUNT_JSON
  folder_id: "0AbCdEf..."
  spreadsheet_selector:
    by_pattern: ["*Master*", "Q4*"]
  header_row: 1
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Setup

1. Create a service account in Google Cloud Console with **Drive API + Sheets API** enabled.
2. Share the target folder (or each spreadsheet) with the service account email as **Viewer**.
3. Download the JSON key and set the full blob as an env var.

## Related

- `google_sheets_resource`
- `google_sheets_ingestion` — single-sheet counterpart
