# EventHubs to Database Asset

Consumes a batch of events from an Azure Event Hub and writes them to a database table via SQLAlchemy. Designed to be paired with `eventhubs_monitor`.

## Required packages

```
azure-eventhub>=5.11.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `connection_string_env_var` | str | yes | — | Env var with Event Hubs connection string |
| `eventhub_name` | str | yes | — | Event Hub name |
| `consumer_group` | str | no | `$Default` | Consumer group |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_events` | int | no | 10000 | Max events to consume per run |
| `max_wait_seconds` | float | no | 5.0 | Max seconds to wait for events |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.EventHubsToDatabaseAssetComponent
attributes:
  asset_name: eventhubs_events_ingest
  connection_string_env_var: EVENTHUB_CONNECTION_STRING
  eventhub_name: my-hub
  consumer_group: $Default
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_events: 10000
```

## Sensor pairing

Pair with `eventhubs_monitor` to trigger a run per partition when new events arrive. The sensor passes `partition_id` and `sequence_number` via `run_config`.
