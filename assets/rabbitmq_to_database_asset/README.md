# RabbitMQ to Database Asset

Drains messages from a RabbitMQ queue and writes them to a database table via SQLAlchemy. Uses `basic_get` for synchronous batch consumption. Messages are acknowledged after a successful write.

## Required packages

```
pika>=1.3.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `amqp_url_env_var` | str | yes | — | Env var with AMQP URL (amqp://user:pass@host/vhost) |
| `queue_name` | str | yes | — | RabbitMQ queue name |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_messages` | int | no | 10000 | Max messages to consume per run |
| `prefetch_count` | int | no | 100 | AMQP prefetch count |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.RabbitMQToDatabaseAssetComponent
attributes:
  asset_name: rabbitmq_orders_ingest
  amqp_url_env_var: RABBITMQ_URL
  queue_name: orders
  database_url_env_var: DATABASE_URL
  table_name: raw_orders
  max_messages: 10000
```
