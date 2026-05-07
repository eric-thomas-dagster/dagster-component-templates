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

## IoT Hub reuse

Azure IoT Hub has a built-in **Event Hubs-compatible endpoint** so this
component works as-is for IoT telemetry ingestion. Get the EH-compatible
connection string from the IoT Hub portal: IoT Hub → Hub Settings →
Built-in endpoints → "Event Hub-compatible endpoint" + "Event Hub-compatible name".

```yaml
attributes:
  asset_name: telemetry_in_postgres
  connection_string_env_var: IOTHUB_EH_COMPATIBLE_CONN_STRING
  eventhub_name: <Event Hub-compatible name>   # NOT the IoT Hub name itself
  database_url_env_var: DATABASE_URL
  table_name: device_telemetry
```

The component uses the standard `azure.eventhub.EventHubConsumerClient`,
which speaks the IoT Hub's EH-compat endpoint without any IoT-specific
SDK. For device-twin / direct-method / file-upload scenarios where you
need the dedicated IoT Hub SDK, use a custom op with `azure-iot-hub`.
