# DataFrame to Snowflake (Bulk)

Memory-efficient bulk load of a pandas DataFrame into a Snowflake table via chunked parquet → PUT to internal stage → `COPY INTO`. Designed for medium-large DataFrames (~1M–50M rows) where `pd.to_sql` is too slow and `write_pandas`'s internal `.copy()` would double peak memory.

## When to use this vs `dataframe_to_snowflake`

| Scenario | Use |
|---|---|
| Small DataFrame (< 1M rows), convenience | `dataframe_to_snowflake` |
| Medium-large DataFrame (1M–50M rows), memory-tight | **this component** |
| Source data too big to fit in a DataFrame at all | `database_replication` (Sling under the hood — never lands in Python memory) |

## How it works

1. Stream the DataFrame to local parquet chunks (`chunk_rows` per file, default 500k)
2. `PUT file://<chunk> @~/<random_stage>` each chunk to an internal user stage
3. `CREATE TABLE … USING TEMPLATE (INFER_SCHEMA …)` from the staged parquet (mode `replace` and `append`)
4. `COPY INTO <table> FROM @stage FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`
5. For `mode: merge`: COPY into a transient staging table, then `MERGE` on `primary_key`
6. Purge stage (configurable)

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream DataFrame asset |
| `table` | `str` | required | Destination Snowflake table |
| `database` / `schema` / `warehouse` / `role` | `str` | `None` | Connection overrides |
| `account_env_var` / `user_env_var` / `password_env_var` | `str` | `SNOWFLAKE_*` | Credential env vars |
| `mode` | `str` | `replace` | `replace`, `append`, or `merge` |
| `primary_key` | `list[str]` | `None` | Required for `mode: merge` |
| `chunk_rows` | `int` | `500000` | Rows per staged parquet file |
| `on_error` | `str` | `ABORT_STATEMENT` | Snowflake `COPY ON_ERROR` option |
| `purge_stage` | `bool` | `true` | Delete staged files after `COPY` |

## Example: append mode

```yaml
type: dagster_component_templates.DataframeToSnowflakeBulkComponent
attributes:
  asset_name: events_loaded
  upstream_asset_key: daily_events
  table: events
  database: ANALYTICS
  schema: PUBLIC
  warehouse: COMPUTE_WH
  mode: append
  chunk_rows: 1000000
```

## Example: merge mode (upsert by primary key)

```yaml
type: dagster_component_templates.DataframeToSnowflakeBulkComponent
attributes:
  asset_name: customers_upserted
  upstream_asset_key: customer_updates
  table: customers
  database: ANALYTICS
  schema: PUBLIC
  warehouse: COMPUTE_WH
  mode: merge
  primary_key: [customer_id]
```

## Metadata emitted

The asset emits these on materialization:

- `row_count`, `dagster/row_count`
- `column_count`
- `mode`
- `chunks` — number of parquet files staged
- `stage_seconds` — total PUT time
- `copy_seconds` — `COPY INTO` (or `MERGE`) time
- `total_seconds`
- `rows_per_second`

Useful for spotting Snowflake warehouse undersizing (low `rows_per_second`) or network bottlenecks (high `stage_seconds / total_seconds` ratio).

## Requirements

```
dagster
pandas>=1.5.0
pyarrow>=11.0.0
snowflake-connector-python[pandas]>=3.0.0
```

## Tips

- For maximum throughput, size Snowflake warehouse to MEDIUM or larger before kicking off — COPY parallelizes across cluster nodes
- For `mode: merge`, the staging table is created as `TRANSIENT` (no Time Travel) — cheaper for one-shot use
- If `purge_stage: false` you can inspect staged files with `LIST @~/...` and re-COPY without re-uploading
