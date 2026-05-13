# File Ingestion

Unified tabular-file → pandas DataFrame ingestion. Replaces the separate `csv_file_ingestion` with a single component that handles CSV, TSV, JSON, JSONL, Parquet, and Excel — with three input modes and full fsspec URI support (so `s3://`, `gs://`, `abfss://` / `abfs://` / `az://` Just Work everywhere a path is expected).

## Three input modes

Exactly one of:

```yaml
# 1. Fixed path / URI — works for local paths and any fsspec scheme
file_path: /data/customers.csv
file_path: s3://my-bucket/customers.csv
file_path: gs://my-bucket/2024/data.parquet
file_path: abfss://container@account.dfs.core.windows.net/2024/data.csv  # ADLS Gen2
```

```yaml
# 2. Resolve from an upstream asset's dict — pairs with archive_fetcher etc.
from_upstream:
  asset: ml_archive
  dict_key: "movies.csv"
  # — or —
  dict_keys: ["movies.csv", "ratings.csv"]
  combine: concat | concat_with_source
  # — or —
  dict_key_pattern: "*.csv"
  combine: concat_with_source
```

```yaml
# 3. Resolve from sensor-injected RunConfig (event-driven flows)
from_run_config:
  uri_template: "s3://{bucket}/{key}"
```

`from_run_config` builds a Pydantic Config class from the `{field}` placeholders in the template. A sensor like `s3_monitor` yields `RunRequest(run_config={"ops": {"<asset_name>": {"config": {"bucket": "...", "key": "..."}}}})` per detected object, and the asset resolves the URI per run.

## Dynamic-partition flow

`{partition_key}` is a special template variable — it resolves to `context.partition_key` instead of a RunConfig field. Combined with `partition_type: dynamic`, this is the pattern for "one materialization per detected file, persistently tracked":

```yaml
type: ...FileIngestionComponent
attributes:
  asset_name: s3_files_processed
  format: auto
  partition_type: dynamic
  dynamic_partition_name: "s3_keys"
  from_run_config:
    uri_template: "s3://my-bucket/{partition_key}"
```

A sensor registers each new S3 key as a dynamic partition (via `instance.add_dynamic_partitions(...)`) and yields `RunRequest(partition_key=key)`. Each file becomes a tracked partition you can reprocess on-demand from the UI.

## Format detection

| `format:` | Reader |
|---|---|
| `auto` | Inferred from URI extension (with `.gz` / `.bz2` / `.xz` / `.zip` stripped first) |
| `csv` | `pd.read_csv` |
| `tsv` | `pd.read_csv(sep='\t')` |
| `json` | `pd.read_json` (one object/array per file) |
| `jsonl` | `pd.read_json(lines=True)` — also accepts `.ndjson` |
| `parquet` | `pd.read_parquet` |
| `excel` | `pd.read_excel` |

## Cloud storage auth

Same fsspec rules as `archive_fetcher`:
- **S3**: env vars / `~/.aws/credentials` / EC2 instance profile / IRSA
- **GCS**: `GOOGLE_APPLICATION_CREDENTIALS` / Workload Identity
- **Azure**: `AZURE_STORAGE_CONNECTION_STRING` / `DefaultAzureCredential`. The recommended scheme is `abfss://container@account.dfs.core.windows.net/path` (canonical ADLS Gen2 over HTTPS). `abfs://` is the HTTP variant; `az://` is the adlfs alias and requires `AZURE_STORAGE_ACCOUNT_NAME` env var.

Install the per-scheme driver: `pip install s3fs` / `gcsfs` / `adlfs`.

## Relationship to csv_file_ingestion

`csv_file_ingestion` still exists and works exactly as before. New projects should prefer `file_ingestion` — it's a strict superset.
