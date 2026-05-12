# dataframe_to_parquet

## Purpose

Writes a Pandas DataFrame to a Parquet file. This is a terminal sink component — it receives a DataFrame from an upstream asset via Dagster's `ins` mechanism and persists it using `DataFrame.to_parquet`. It supports local paths, S3 (`s3://`), and GCS (`gs://`) destinations. It returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, and `compression` metadata.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the DataFrame |
| `file_path` | `str` | required | Destination path. Supports env var substitution, `s3://`, and `gs://` paths. |
| `compression` | `str` | `"snappy"` | Compression codec: `"snappy"`, `"gzip"`, `"brotli"`, or `"none"` |
| `partition_cols` | `List[str]` | `None` | Columns to partition by. Creates a Hive-style directory structure. |
| `index` | `bool` | `False` | Include the DataFrame index in output |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToParquetComponent
attributes:
  asset_name: write_events_parquet
  upstream_asset_key: processed_events
  file_path: ${OUTPUT_DIR}/events.parquet
  compression: snappy
  partition_cols:
    - year
    - month
  index: false
  group_name: output
```

## Notes

### Cloud Storage

For S3 paths (`s3://bucket/path.parquet`), install `s3fs` in addition to `pyarrow`. For GCS paths (`gs://bucket/path.parquet`), install `gcsfs`. The component skips local directory creation for remote paths.

### Partitioned Output

When `partition_cols` is set, `to_parquet` writes a Hive-partitioned directory structure rather than a single file. This requires `pyarrow` as the engine.

### Compression

`"snappy"` is the recommended default for a good balance of speed and compression ratio. Use `"gzip"` for better compression at the cost of write speed. Use `"none"` to disable compression entirely.

### Environment Variable Substitution

The `file_path` supports shell-style environment variable substitution via `os.path.expandvars`.

### Materialization Metadata

This component returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, and `compression` metadata.

### Requirements

Install `pandas` and `pyarrow`. For S3: also install `s3fs`. For GCS: also install `gcsfs`.
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
