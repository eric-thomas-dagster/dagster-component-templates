# SQL to Database Asset

Reads rows from a source database table or custom SQL query and writes them to a destination database table via SQLAlchemy. Supports full-table copy, incremental watermark-based loads, and cross-database migrations.

Works with any SQLAlchemy-compatible database: Postgres, MySQL, MSSQL, SQLite, Snowflake, BigQuery, Redshift, DuckDB, and more.

## Required packages

```
pandas>=1.3.0
sqlalchemy>=2.0.0
# plus any database-specific driver (psycopg2, snowflake-sqlalchemy, etc.)
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `source_url_env_var` | str | yes | — | Env var with source SQLAlchemy database URL |
| `destination_url_env_var` | str | yes | — | Env var with destination SQLAlchemy database URL |
| `source_table` | str | no | null | Source table name (use source_table OR source_query) |
| `source_schema` | str | no | null | Source schema name |
| `source_query` | str | no | null | Custom SQL query (overrides source_table) |
| `destination_table` | str | yes | — | Destination table name |
| `destination_schema` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `watermark_column` | str | no | null | Incremental watermark column (e.g. updated_at, id) |
| `watermark_env_var` | str | no | null | Env var storing the last watermark value |
| `chunksize` | int | no | 10000 | Rows to read/write per chunk |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.SQLToDatabaseAssetComponent
attributes:
  asset_name: crm_contacts_sync
  source_url_env_var: SOURCE_DB_URL
  destination_url_env_var: DESTINATION_DB_URL
  source_table: contacts
  source_schema: public
  destination_table: raw_contacts
  watermark_column: updated_at
  watermark_env_var: CONTACTS_WATERMARK
```
