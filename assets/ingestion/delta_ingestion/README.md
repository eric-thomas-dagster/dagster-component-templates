# Delta Lake Ingestion Component

Read from an **existing external** Delta Lake table — one written by Spark, Databricks, Trino, Flink, or any other engine — into a Dagster asset as a pandas DataFrame.

Uses `delta-rs` (the Rust implementation, exposed via the `deltalake` Python package). **No Spark, no JVM** — just a Python wheel.

## When this is the right component

| | Use this | Use [`delta_lake_io_manager`](https://dagster-community-components-cli.vercel.app/c/delta_lake_io_manager) / [`deltalake_polars_io_manager`](https://dagster-community-components-cli.vercel.app/c/deltalake_polars_io_manager) |
|---|---|---|
| Who owns the table | Some other engine | Dagster |
| What you do | Read existing versions | Materialize from scratch each run |
| Pattern | "Cross-engine lake — Dagster is one reader" | "Dagster pipeline writes Delta as output" |
| Library | `deltalake` (delta-rs) | `dagster_deltalake` (wraps delta-rs as IO manager) |

The official `dagster_deltalake` package ships only IO managers (`DeltaLakePyarrowIOManager`, `DeltaLakePandasIOManager`) + a `DeltaTableResource`. This component fills the **read-from-existing** gap.

## Storage backends

| Backend | URI scheme | Storage options |
|---|---|---|
| **S3** | `s3://bucket/path` | `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` (for MinIO/R2) |
| **ADLS Gen2** | `az://container@account.dfs.core.windows.net/path` | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_KEY` (or `AZURE_CLIENT_ID` / Workload Identity) |
| **GCS** | `gs://bucket/path` | `GOOGLE_SERVICE_ACCOUNT_KEY` |
| **Local** | `/local/path` | — |
| **Unity Catalog** | `uc://catalog.schema.table` | `UC_TOKEN`, `UC_HOST` |

All values support `${ENV_VAR}` expansion at runtime.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | |
| `table_uri` | `str` | yes | Delta table location |
| `storage_options` | `dict` | conditional | Cloud creds + config |
| `select_columns` | `list` | no | Projection |
| `partition_filters` | `list[list]` | no | `[[col, op, val], ...]`. Supports `{partition_key}` |
| `version` | `int` | no | Time travel: specific Delta version |
| `timestamp` | `str` | no | Time travel: ISO timestamp (mutually exclusive with `version`) |
| Standard fields | | | partition / freshness / owners / tags / deps / retry |

## Partition predicate pushdown

Delta Lake supports cheap partition-level filtering. The component pushes these to `delta-rs`:

```yaml
partition_filters:
  - [event_date, "=", "{partition_key}"]
  - [region, "in", "us-east-1,us-west-2"]
```

Only the files in matching partitions are read — massive perf win on large tables.

Supported ops: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`.

## Time travel

Two ways to read a non-latest version:

```yaml
# Specific version (deterministic — best for re-runs)
version: 42

# OR: version active at a timestamp
timestamp: '2024-01-15T00:00:00Z'
```

For backfills, prefer `version` — Delta vacuums can compact away the data files referenced by old versions if retention is short. Versions are stable; timestamps less so.

## Unity Catalog

For Databricks Unity Catalog tables, use the `uc://` URI scheme and pass UC credentials in `storage_options`:

```yaml
table_uri: uc://main.sales.events
storage_options:
  UC_TOKEN: "${DATABRICKS_TOKEN}"
  UC_HOST: "https://my-workspace.cloud.databricks.com"
```

This avoids needing the raw S3/ADLS path — UC resolves the storage location automatically.

## Dependencies

```
deltalake>=0.15.0
pyarrow>=10.0.0
pandas>=1.5.0
```

That's it. No Spark, no JVM. Lightweight enough for k8s pods + serverless functions.

## See also

- [`delta_lake_io_manager`](https://dagster-community-components-cli.vercel.app/c/delta_lake_io_manager) — when Dagster OWNS the table
- [`deltalake_polars_io_manager`](https://dagster-community-components-cli.vercel.app/c/deltalake_polars_io_manager) — Polars variant
- [`dataframe_to_delta_table`](../../sinks/dataframe_to_delta_table/) — write to an existing Delta table
- [`external_delta_table`](../../../external_assets/external_delta_table/) — declare a Delta table as an observable asset
- [`iceberg_ingestion`](../iceberg_ingestion/) — sister component for Apache Iceberg
- [`dataframe_to_databricks`](https://dagster-community-components-cli.vercel.app/c/dataframe_to_databricks) — Databricks-specific Delta sink
