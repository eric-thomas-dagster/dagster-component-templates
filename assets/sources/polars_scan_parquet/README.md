# Polars Scan Parquet (predicate + projection pushdown)

Read parquet via polars's lazy scanner. When you supply `predicate:` and/or `columns:`, polars's query planner pushes them down to the parquet reader — only matching row groups + selected columns are read off disk/object-store. That's true predicate pushdown, which the in-memory `filter(backend=polars)` component CAN'T deliver.

## Use when

- Source data is parquet (local FS, S3, GCS, ADLS)
- You want only a subset of rows / columns
- The full frame is too large to load
- You need cloud-storage-native reads without going through pandas

## YAML

```yaml
type: dagster_component_templates.PolarsScanParquetComponent
attributes:
  asset_name: paid_orders_recent
  path: s3://bucket/orders/year=2026/*.parquet
  columns: [order_id, customer_id, amount, status, created_at]
  predicate: "status = 'paid' AND created_at >= '2026-01-01'"
  storage_options:
    aws_access_key_id: AKIA...
    aws_secret_access_key: ...
    region: us-east-1
```

## Path support

Glob patterns, local paths, and cloud URIs all work — polars's `scan_parquet` handles them natively:

| Scheme | Example |
|---|---|
| Local | `/data/orders/*.parquet` |
| S3 | `s3://bucket/orders/**/*.parquet` |
| GCS | `gs://bucket/path/` |
| ADLS | `az://container@account.dfs.core.windows.net/path/` |

## What gets pushed down

- **Column projection** — `columns:` becomes a parquet column filter. Unread columns never come off disk.
- **Predicate** — `predicate:` becomes a row-group skip filter. Polars uses parquet column statistics (min/max per row group) to skip groups that can't match the predicate.
- **`n_rows:`** — LIMIT pushdown. The reader stops after producing this many rows.

The actual pushdown coverage depends on the predicate. Range/equality predicates on indexed columns push best. Predicates against computed columns can't push.

## Output

`output_type: polars` (default) returns a polars DataFrame for the next polars step. `output_type: pandas` converts at the boundary for pandas-only consumers.

## Streaming

`streaming: true` uses polars's streaming engine for out-of-core execution (frames larger than memory). Combine with predicate/column pushdown for the full effect — only matching pages are read, and they're consumed in batches without holding the whole frame in memory.
