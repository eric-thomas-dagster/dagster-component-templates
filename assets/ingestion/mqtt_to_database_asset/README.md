# MQTT to Database Asset

Subscribes to an MQTT topic, collects messages for a configured duration, then writes them to a database table via SQLAlchemy. Each message payload is expected to be JSON. The `_topic` field is automatically added to each record.

## Required packages

```
paho-mqtt>=1.6.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `broker_host_env_var` | str | yes | — | Env var with MQTT broker hostname |
| `broker_port` | int | no | 1883 | MQTT broker port (1883 or 8883 for TLS) |
| `topic` | str | yes | — | MQTT topic to subscribe to (supports # and +) |
| `username_env_var` | str | no | null | Env var with MQTT username |
| `password_env_var` | str | no | null | Env var with MQTT password |
| `use_tls` | bool | no | false | Enable TLS/SSL connection |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `collect_seconds` | float | no | 30.0 | Seconds to collect messages before writing |
| `max_messages` | int | no | 10000 | Max messages to collect per run |
| `qos` | int | no | 1 | MQTT QoS level (0, 1, or 2) |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.MQTTToDatabaseAssetComponent
attributes:
  asset_name: mqtt_sensors_ingest
  broker_host_env_var: MQTT_BROKER_HOST
  topic: sensors/#
  database_url_env_var: DATABASE_URL
  table_name: raw_sensor_readings
  collect_seconds: 30
  max_messages: 10000
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
