# GCS to Database

Read a file from Google Cloud Storage and write it to a database table. Designed to be triggered by `gcs_monitor` passing source info via `run_config`.

## Pattern

```
gcs_monitor  →  [Dagster job]  →  gcs_to_database_asset
```

## Example

```yaml
type: dagster_component_templates.GCSToDatabaseAssetComponent
attributes:
  asset_name: gcs_events_ingest
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  file_format: parquet
  if_exists: append
```

## Destination

All components write via SQLAlchemy — compatible with PostgreSQL, MySQL, Snowflake (`snowflake-sqlalchemy`), BigQuery (`sqlalchemy-bigquery`), Redshift (`sqlalchemy-redshift`), DuckDB, and SQLite.

Set `if_exists` to:
- `append` — add rows (default)
- `replace` — drop and recreate the table
- `fail` — raise if table exists

## Dependencies

```
google-cloud-storage>=2.0.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```
