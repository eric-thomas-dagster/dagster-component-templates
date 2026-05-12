# dataframe_from_csv

## Purpose

Reads a CSV file from the local filesystem and outputs a Pandas DataFrame asset. Supports environment variable substitution in the file path, making it easy to configure different paths across environments (development, staging, production) without changing the component YAML.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `file_path` | `str` | required | Path to CSV file. Supports env var substitution, e.g. `${DATA_DIR}/file.csv` |
| `delimiter` | `str` | `","` | Column delimiter character |
| `encoding` | `str` | `"utf-8"` | File encoding |
| `parse_dates` | `List[str]` | `None` | Columns to parse as dates |
| `dtype` | `dict` | `None` | Column dtype overrides, e.g. `{id: str, amount: float}` |
| `skiprows` | `int` | `None` | Number of rows to skip at the start of the file |
| `nrows` | `int` | `None` | Maximum number of rows to read |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeFromCsvComponent
attributes:
  asset_name: sales_data
  file_path: "${DATA_DIR}/sales/2024_sales.csv"
  delimiter: ","
  encoding: utf-8
  parse_dates:
    - order_date
    - ship_date
  dtype:
    order_id: str
    amount: float
  skiprows: 1
  nrows: 10000
  group_name: ingestion
```

## Notes

### Environment Variable Substitution

The `file_path` field supports shell-style environment variable substitution via `os.path.expandvars`. For example, `${DATA_DIR}/sales/file.csv` will expand `DATA_DIR` from the environment at runtime. This allows the same YAML to work across different deployments.

### Date Parsing

Columns listed in `parse_dates` are passed directly to `pd.read_csv`. Pandas will attempt to infer the date format automatically. For non-standard formats, consider using a downstream transform component to parse dates explicitly.

### dtype Overrides

Use `dtype` to enforce column types at read time, which is more efficient than casting later. Common use cases include keeping ID columns as strings to prevent integer overflow, or ensuring numeric columns are read as `float` rather than `int`.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
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
