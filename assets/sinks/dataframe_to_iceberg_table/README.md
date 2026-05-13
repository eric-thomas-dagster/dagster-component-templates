# Dataframe → Iceberg Table Sink

Write a pandas DataFrame to an **existing** Apache Iceberg table — one shared across engines (Snowflake / Trino / Spark / Flink / Databricks / etc.). Reverse direction of [`iceberg_ingestion`](../../ingestion/iceberg_ingestion/).

## When this is the right component

| | Use this | Use [`iceberg_io_manager`](https://dagster-community-components-cli.vercel.app/c/iceberg_io_manager) |
|---|---|---|
| Table is shared with other engines | ✅ | ❌ |
| Dagster fully owns the table | ❌ | ✅ |
| Need append / partition-level overwrite | ✅ | ⚠️ depends on IO manager mode |

## Modes

| Mode | What it does | When |
|---|---|---|
| `append` | Add new rows to the table | Incremental ingestion, idempotent backfills with date partitions |
| `overwrite` | Replace ALL rows | Full refreshes |
| `overwrite` + `overwrite_filter` | Replace rows matching the filter | Partition-level overwrite (e.g. re-run a daily partition) |

PyIceberg's MERGE / upsert support is limited. For upserts, use Spark / Trino / Snowflake SQL — or the equivalent engine the table's primary writer uses.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` / `upstream_asset_key` | `str` | yes | |
| `catalog_type` | `str` | no | `rest` (default) / `glue` / `hive` / `hadoop` / `sql` |
| `catalog_properties` | `dict` | yes | Mirrors `iceberg_ingestion` |
| `namespace` / `table_name` | `str` | yes | |
| `mode` | `str` | no | `append` (default) / `overwrite` |
| `overwrite_filter` | `str` | no | Iceberg expression — partition-level overwrite. Supports `{partition_key}` |
| `skip_columns` | `list` | no | Drop these from DF before writing |
| `schema_evolution` | `bool` | no | Allow new columns. Default false (safer) |

## Partition-level overwrite pattern

The right pattern for "re-run a day's worth of data without duplicating":

```yaml
mode: overwrite
overwrite_filter: order_date = '{partition_key}'
partition_type: daily
partition_start: '2024-01-01'
```

Each daily run replaces just that day's rows. Re-running a partition is idempotent.

## See also

- [`iceberg_ingestion`](../../ingestion/iceberg_ingestion/) — read side
- [`iceberg_io_manager`](https://dagster-community-components-cli.vercel.app/c/iceberg_io_manager) — Dagster-owned table (output via IO manager)
- [`dataframe_to_delta_table`](../dataframe_to_delta_table/) — sister sink for Delta Lake
- [`external_iceberg_table`](../../../external_assets/external_iceberg_table/) — observable external asset
