# Dataframe → Delta Table Sink

Write a pandas DataFrame to an **existing** Delta Lake table using `delta-rs` (no Spark / JVM). For Delta tables shared with other engines.

## When this is the right component

| | Use this | Use [`dataframe_to_databricks`](https://dagster-community-components-cli.vercel.app/c/dataframe_to_databricks) | Use [`delta_lake_io_manager`](https://dagster-community-components-cli.vercel.app/c/delta_lake_io_manager) |
|---|---|---|---|
| Where it writes | Any S3/ADLS/GCS/UC Delta table | Databricks workspace SQL warehouse | Local / S3 / ADLS, Dagster-owned |
| Engine | delta-rs (Rust, lightweight) | Databricks SQL connector | delta-rs via official IO manager |
| Use when | Cross-engine Delta on S3 / ADLS / GCS / UC | Specifically targeting Databricks SQL warehouse | Dagster owns the table |

## Modes

| Mode | What it does | Required fields |
|---|---|---|
| `append` (default) | Add rows to the table | — |
| `overwrite` | Replace ALL rows | — |
| `overwrite` + `overwrite_partition_filter` | Replace rows in matching partitions | `overwrite_partition_filter` |
| `merge` | Upsert: match on key → update existing, insert new | `merge_predicate` |
| `error` / `ignore` | Fail if table exists / silently no-op | — |

## Partition-level overwrite (idempotent backfill)

```yaml
mode: overwrite
overwrite_partition_filter:
  - [event_date, "=", "{partition_key}"]
partition_type: daily
partition_start: '2024-01-01'
```

Each daily run replaces just that day's rows. Re-running is safe.

## Merge (basic upsert)

```yaml
mode: merge
merge_predicate: "target.event_id = source.event_id"
```

The component does `when_matched_update_all()` + `when_not_matched_insert_all()`. For more complex MERGE logic (column-specific updates, deletes-when-matched, conditional inserts), drop down to a custom Python asset using `dt.merge(...)` directly.

## Storage backends

| Backend | URI | Storage options |
|---|---|---|
| S3 | `s3://bucket/path` | `AWS_REGION`, `AWS_ACCESS_KEY_ID`, etc. |
| ADLS | `az://container@account.dfs.core.windows.net/path` | `AZURE_STORAGE_*` |
| GCS | `gs://bucket/path` | `GOOGLE_SERVICE_ACCOUNT_KEY` |
| Unity Catalog | `uc://catalog.schema.table` | `UC_TOKEN`, `UC_HOST` |
| Local | `/local/path` | — |

`${ENV_VAR}` expansion supported.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` / `upstream_asset_key` | `str` | yes | |
| `table_uri` | `str` | yes | |
| `storage_options` | `dict` | conditional | |
| `mode` | `str` | no | `append` (default) / `overwrite` / `merge` / `error` / `ignore` |
| `partition_by` | `list` | no | Must match existing table partitioning |
| `overwrite_partition_filter` | `list[list]` | conditional | `[[col, op, val], ...]` |
| `merge_predicate` | `str` | conditional | For `mode: merge` |
| `schema_mode` | `str` | no | `merge` to allow new columns |
| `skip_columns` | `list` | no | |

## See also

- [`delta_ingestion`](../../ingestion/delta_ingestion/) — read side
- [`dataframe_to_databricks`](https://dagster-community-components-cli.vercel.app/c/dataframe_to_databricks) — Databricks-specific Delta sink
- [`delta_lake_io_manager`](https://dagster-community-components-cli.vercel.app/c/delta_lake_io_manager) — Dagster-owned Delta via IO manager
- [`dataframe_to_iceberg_table`](../dataframe_to_iceberg_table/) — sister sink for Iceberg
- [`external_delta_table`](../../../external_assets/external_delta_table/) — observable external asset
