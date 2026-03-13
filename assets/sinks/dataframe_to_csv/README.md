# dataframe_to_csv

## Purpose

Writes a Pandas DataFrame to a CSV file. This is a terminal sink component — it receives a DataFrame from an upstream asset via Dagster's `ins` mechanism and persists it using `DataFrame.to_csv`. It returns a `MaterializeResult` with `row_count`, `column_count`, and `file_path` metadata visible in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the DataFrame |
| `file_path` | `str` | required | Destination file path. Supports env var substitution e.g. `${OUTPUT_DIR}/results.csv` |
| `delimiter` | `str` | `","` | Column delimiter |
| `include_index` | `bool` | `False` | Include row index in output |
| `encoding` | `str` | `"utf-8"` | File encoding |
| `columns` | `List[str]` | `None` | Subset of columns to write. If None, all columns are written. |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: write_monthly_report_csv
  upstream_asset_key: monthly_revenue_summary
  file_path: ${OUTPUT_DIR}/monthly_report.csv
  delimiter: ","
  include_index: false
  encoding: utf-8
  columns:
    - date
    - revenue
    - region
  group_name: output
```

## Notes

### Environment Variable Substitution

The `file_path` supports shell-style environment variable substitution via `os.path.expandvars`. For example, `${OUTPUT_DIR}/results.csv` will be expanded using the `OUTPUT_DIR` environment variable at runtime.

### Column Selection

Use `columns` to write only a subset of the upstream DataFrame's columns. This is useful for stripping intermediate columns before writing the final output.

### Directory Creation

The component automatically creates parent directories if they do not exist.

### Materialization Metadata

This component returns a `MaterializeResult` with `row_count`, `column_count`, and `file_path` metadata. These are visible in the Dagster UI on the asset's materialization page.

### IO Manager

This component uses Dagster's `ins` mechanism to receive the upstream DataFrame. No IO manager configuration is required for local development — Dagster's default `FilesystemIOManager` handles serialization automatically.

### Requirements

Install `pandas`. No additional dependencies are required.
