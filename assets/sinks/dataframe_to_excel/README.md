# dataframe_to_excel

## Purpose

Writes a Pandas DataFrame to an Excel `.xlsx` file. This is a terminal sink component — it receives a DataFrame from an upstream asset via Dagster's `ins` mechanism and persists it using `DataFrame.to_excel`. Optionally supports freezing panes via `openpyxl`. It returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, and `sheet_name` metadata.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the DataFrame |
| `file_path` | `str` | required | Destination file path. Supports env var substitution e.g. `${OUTPUT_DIR}/report.xlsx` |
| `sheet_name` | `str` | `"Sheet1"` | Target worksheet name |
| `include_index` | `bool` | `False` | Include row index in output |
| `columns` | `List[str]` | `None` | Subset of columns to write. If None, all columns are written. |
| `freeze_panes` | `List[int]` | `None` | Freeze rows/columns as `[row, col]` (0-based). e.g. `[1, 0]` freezes the first row. |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToExcelComponent
attributes:
  asset_name: write_quarterly_report_excel
  upstream_asset_key: quarterly_summary
  file_path: ${OUTPUT_DIR}/quarterly_report.xlsx
  sheet_name: Summary
  include_index: false
  columns:
    - quarter
    - revenue
    - expenses
    - profit
  freeze_panes:
    - 1
    - 0
  group_name: output
```

## Notes

### Timezone-aware datetimes

Excel cannot store tz-aware datetime values. When the upstream DataFrame contains any tz-aware column, the component automatically converts it to UTC and drops the timezone (preserving the wall-clock value) before writing, and logs which columns were stripped. No configuration needed.

### Freeze Panes

When `freeze_panes` is set, the component uses `openpyxl` as the Excel engine and applies the freeze via `worksheet.freeze_panes`. The value `[1, 0]` freezes the first header row; `[1, 1]` freezes both the first row and first column.

### Column Selection

Use `columns` to write only a subset of the upstream DataFrame's columns.

### Environment Variable Substitution

The `file_path` supports shell-style environment variable substitution via `os.path.expandvars`.

### Directory Creation

The component automatically creates parent directories if they do not exist.

### Materialization Metadata

This component returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, and `sheet_name` metadata.

### IO Manager

This component uses Dagster's `ins` mechanism to receive the upstream DataFrame. No IO manager configuration is required for local development.

### Requirements

Install `pandas` and `openpyxl`.
## ⚠️ Deployment note (Dagster+ / Kubernetes)

This component reads or writes local filesystem paths. Behavior across deployments:

| Environment | Works? |
|---|---|
| Local dev | ✅ Yes |
| Dagster+ Serverless (multiprocess executor, default) | ✅ Within a single run — `/tmp/...` is shared across ops in the same run. Files do **not** persist after the run ends. |
| Dagster Hybrid on k8s with `k8s_job` executor (op-per-pod) | ❌ Each op runs in its own pod with its own `/tmp` — files don't travel between ops, even within one run. Set the run to use the `in_process` executor as a workaround. |
| Cross-run reads (run N writes, run N+1 reads) | ❌ Anywhere — the local filesystem is ephemeral by definition. |

**Recommended alternatives for production:**

1. **Return bytes as the asset value** instead of writing a file. The default `PickledObjectFilesystemIOManager` (and the Dagster+ Serverless S3-backed IO manager) serialize binary data fine. Downstream ops read the bytes from the IO manager regardless of pod / run.
2. **Use a cloud-storage sink** for cross-run persistence: [`dataframe_to_s3`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_s3), [`dataframe_to_gcs`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_gcs), [`dataframe_to_adls`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_adls).
3. **Mount a shared volume** (k8s PVC / Cloud Run volumes) if you genuinely need a shared filesystem path across pods.
