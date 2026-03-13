# ADLS to Database

Read a file from Azure Data Lake Storage and write it to a database table. Designed to be triggered by `adls_monitor` passing source info via `run_config`.

## Pattern

```
adls_monitor  →  [Dagster job]  →  adls_to_database_asset
```

## Example

```yaml
type: dagster_component_templates.ADLSToDatabaseAssetComponent
attributes:
  asset_name: adls_orders_ingest
  connection_string_env_var: AZURE_STORAGE_CONNECTION_STRING
  database_url_env_var: DATABASE_URL
  table_name: raw_orders
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
azure-storage-blob>=12.0.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
