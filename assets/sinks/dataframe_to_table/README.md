# dataframe_to_table

## Purpose

Writes a Pandas DataFrame to a relational database table. This is a terminal sink component — it receives a DataFrame from an upstream asset via Dagster's `ins` mechanism and persists it using `DataFrame.to_sql`. It returns a `MaterializeResult` with `row_count` metadata visible in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the DataFrame |
| `table_name` | `str` | required | Destination database table name |
| `database_url_env_var` | `str` | `"DATABASE_URL"` | Environment variable containing the DB connection URL |
| `if_exists` | `str` | `"replace"` | Behavior if the table already exists: `replace`, `append`, or `fail` |
| `schema` | `str` | `None` | Database schema |
| `chunksize` | `int` | `None` | Rows per batch when writing (useful for large DataFrames) |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToTableComponent
attributes:
  asset_name: write_monthly_summary
  upstream_asset_key: monthly_revenue_summary
  table_name: monthly_revenue_summary
  database_url_env_var: DATABASE_URL
  if_exists: replace
  schema: reporting
  chunksize: 1000
  group_name: output
```

## Notes

### if_exists Behavior

- `replace`: Drops the existing table and recreates it (schema is derived from the DataFrame).
- `append`: Inserts rows into the existing table without modifying the schema.
- `fail`: Raises an error if the table already exists.

`replace` is the safest default for idempotent pipelines.

### Chunked Writing

For large DataFrames, set `chunksize` to write in batches rather than all at once. This reduces peak memory usage and can improve reliability on slower database connections.

### Materialization Metadata

This component returns a `MaterializeResult` with `row_count` metadata. The row count is visible in the Dagster UI on the asset's materialization page.

### IO Manager

This component uses Dagster's `ins` mechanism to receive the upstream DataFrame. The IO manager deserializes the DataFrame automatically — no additional configuration is needed. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

### Requirements

Install `sqlalchemy` and the appropriate database driver (e.g., `psycopg2` for PostgreSQL, `pymysql` for MySQL).
