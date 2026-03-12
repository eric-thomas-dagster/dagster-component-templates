# SFTP to Database

Download a file from SFTP and write it to a database table. Designed to be triggered by `sftp_monitor` passing source info via `run_config`.

## Pattern

```
sftp_monitor  →  [Dagster job]  →  sftp_to_database_asset
```

## Example

```yaml
type: dagster_component_templates.SFTPToDatabaseAssetComponent
attributes:
  asset_name: sftp_orders_ingest
  host_env_var: SFTP_HOST
  username_env_var: SFTP_USERNAME
  password_env_var: SFTP_PASSWORD
  database_url_env_var: DATABASE_URL
  table_name: raw_orders
  file_format: csv
```

## Destination

All components write via SQLAlchemy — compatible with PostgreSQL, MySQL, Snowflake (`snowflake-sqlalchemy`), BigQuery (`sqlalchemy-bigquery`), Redshift (`sqlalchemy-redshift`), DuckDB, and SQLite.

Set `if_exists` to:
- `append` — add rows (default)
- `replace` — drop and recreate the table
- `fail` — raise if table exists

## Dependencies

```
paramiko>=3.0.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```
