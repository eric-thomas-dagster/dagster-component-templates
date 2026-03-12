# SQS to Database

Drain messages from an SQS queue and write them to a database table. Designed to be triggered by `sqs_monitor` passing source info via `run_config`.

## Pattern

```
sqs_monitor  →  [Dagster job]  →  sqs_to_database_asset
```

## Example

```yaml
type: dagster_component_templates.SQSToDatabaseAssetComponent
attributes:
  asset_name: sqs_events_ingest
  queue_url_env_var: SQS_QUEUE_URL
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_messages: 10000
  region_name: us-east-1
```

## Destination

All components write via SQLAlchemy — compatible with PostgreSQL, MySQL, Snowflake (`snowflake-sqlalchemy`), BigQuery (`sqlalchemy-bigquery`), Redshift (`sqlalchemy-redshift`), DuckDB, and SQLite.

Set `if_exists` to:
- `append` — add rows (default)
- `replace` — drop and recreate the table
- `fail` — raise if table exists

## Dependencies

```
boto3>=1.26.0
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
