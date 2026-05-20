# File Transformer Asset

Transform files between formats (CSV, JSON, Parquet, Excel) with optional data processing.

## Overview

Transform files automatically when triggered by file sensors, or process files on a schedule. Supports:
- **Formats**: CSV ↔ JSON ↔ Parquet ↔ Excel
- **Auto-detection**: Automatically detect input format
- **Data Processing**: Drop duplicates, fill NA values, select columns
- **Run Config Support**: Works seamlessly with Filesystem Sensor

## Quick Examples

### With Filesystem Sensor (Automatic)

```yaml
# Sensor detects CSV files, asset converts to Parquet
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: csv_to_parquet
  output_format: parquet
  output_directory: /data/processed
```

### Standalone (Fixed File)

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: transform_data
  input_file_path: /data/input/data.csv
  output_format: parquet
  output_directory: /data/output
```

### With Data Processing

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: clean_and_convert
  output_format: parquet
  output_directory: /data/clean
  drop_duplicates: true
  columns_to_keep: "id,name,date,amount"
  fill_na_value: "0"
```

## Configuration

### Required
- **asset_name** - Asset name
- **output_format** - Output format: `csv`, `json`, `parquet`, `excel`
- **output_directory** - Output directory path

### Optional
- **input_file_path** - Fixed input file (not needed with sensor)
- **input_format** - Input format or `auto` (default: `auto`)
- **output_filename** - Custom output filename
- **drop_duplicates** - Remove duplicate rows (default: `false`)
- **fill_na_value** - Value to replace NaN
- **columns_to_keep** - Comma-separated column list
- CSV/JSON/Parquet/Excel specific options

## Run Config Schema

When triggered by Filesystem Sensor:

```python
{
  "ops": {
    "config": {
      "file_path": str,      # From sensor
      "file_name": str,      # From sensor
      # ... other sensor data
    }
  }
}
```

## Complete Pipeline Example

### 1. Filesystem Sensor

```yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: watch_csv_files
  directory_path: /data/incoming
  file_pattern: ".*\\.csv$"
  job_name: convert_files_job
```

### 2. File Transformer Asset

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: convert_to_parquet
  output_format: parquet
  output_directory: /data/processed
  parquet_compression: snappy
```

### 3. Job

```yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: convert_files_job
  asset_selection: ["convert_to_parquet"]
```

## Supported Transformations

- CSV → Parquet (most common)
- CSV → JSON
- JSON → Parquet
- Excel → CSV
- Parquet → CSV
- Any format to any format

## Requirements

- pandas >= 2.0.0
- pyarrow >= 10.0.0
- openpyxl >= 3.0.0 (for Excel support)

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).

## License

MIT License
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

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset |
| `output_format` | `str` | Output file format: 'csv', 'json', 'parquet', 'excel' |
| `output_directory` | `str` | Directory to write transformed files |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | — | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `list[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. Defines a RetryPolicy when set. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `input_file_path` | `str` | — | Fixed input file path (optional, can use run_config instead) |
| `input_format` | `str` | `"auto"` | Input file format: 'auto', 'csv', 'json', 'parquet', 'excel' |
| `output_filename` | `str` | — | Output filename (optional, defaults to input filename with new extension) |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream DataFrame asset key. When set, the component reads the DataFrame from the upstream asset (no file I/O on input) and writes it to output_directory in the requested output_format. Mutually exclusive with input_file_path / run_config-driven file paths. |
| `csv_delimiter` | `str` | `","` | CSV delimiter character |
| `csv_encoding` | `str` | `"utf-8"` | CSV file encoding |
| `json_orient` | `str` | `"records"` | JSON orientation: 'records', 'split', 'index', 'columns', 'values' |
| `excel_sheet_name` | `str` | `"Sheet1"` | Excel sheet name for reading/writing |
| `parquet_compression` | `str` | `"snappy"` | Parquet compression: 'snappy', 'gzip', 'brotli', None |
| `drop_duplicates` | `bool` | `false` | Whether to drop duplicate rows |
| `fill_na_value` | `str` | — | Value to fill NaN values with (optional) |
| `columns_to_keep` | `str` | — | Comma-separated list of columns to keep (optional) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 25 rows or a sample) for builder UIs. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata. For long DataFrames (>10x preview_rows), a random sample is used; otherwise head(). |

<!-- FIELDS:END -->
