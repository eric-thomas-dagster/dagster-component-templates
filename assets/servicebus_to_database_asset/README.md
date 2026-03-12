# Service Bus to Database Asset

Drains messages from an Azure Service Bus queue or topic subscription and writes them to a database table via SQLAlchemy. Designed to be paired with `servicebus_monitor`.

## Required packages

```
azure-servicebus>=7.11.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `connection_string_env_var` | str | yes | — | Env var with Service Bus connection string |
| `queue_name` | str | no | null | Queue name (use queue_name OR topic+subscription) |
| `topic_name` | str | no | null | Topic name |
| `subscription_name` | str | no | null | Subscription name (required with topic_name) |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_messages` | int | no | 5000 | Max messages to consume per run |
| `max_wait_seconds` | float | no | 5.0 | Max seconds to wait per receive call |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.ServiceBusToDatabaseAssetComponent
attributes:
  asset_name: servicebus_orders_ingest
  connection_string_env_var: SERVICEBUS_CONNECTION_STRING
  queue_name: orders-queue
  database_url_env_var: DATABASE_URL
  table_name: raw_orders
  max_messages: 5000
```
