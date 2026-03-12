# Kafka to Database

Consume a batch of messages from a Kafka topic and write them to a database table. Designed to be triggered by `kafka_monitor` passing source info via `run_config`.

## Pattern

```
kafka_monitor  →  [Dagster job]  →  kafka_to_database_asset
```

## Example

```yaml
type: dagster_component_templates.KafkaToDatabaseAssetComponent
attributes:
  asset_name: kafka_events_ingest
  bootstrap_servers_env_var: KAFKA_BOOTSTRAP_SERVERS
  database_url_env_var: DATABASE_URL
  topic: events
  table_name: raw_events
  max_messages: 10000
  consumer_group: dagster-ingestion
```

## Destination

All components write via SQLAlchemy — compatible with PostgreSQL, MySQL, Snowflake (`snowflake-sqlalchemy`), BigQuery (`sqlalchemy-bigquery`), Redshift (`sqlalchemy-redshift`), DuckDB, and SQLite.

Set `if_exists` to:
- `append` — add rows (default)
- `replace` — drop and recreate the table
- `fail` — raise if table exists

## Dependencies

```
confluent-kafka>=2.0.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```
