# ClickHouse Table Observation Sensor

Periodically connects to ClickHouse and emits `AssetObservation` events for an external ClickHouse table. Captures row count, disk size, active parts count, engine type, and last modified timestamp from ClickHouse system tables.

Connect via a `ClickHouseResource` (using `resource_key`) or directly via env vars.

## Required packages

```
clickhouse-connect>=0.6.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `sensor_name` | Yes | — | Unique sensor name |
| `asset_key` | Yes | — | Asset key of the external ClickHouse table to observe |
| `database` | Yes | — | ClickHouse database name |
| `table` | Yes | — | ClickHouse table name |
| `host_env_var` | No | `null` | Env var with ClickHouse host (used when `resource_key` is not set) |
| `port` | No | `8443` | ClickHouse port |
| `username_env_var` | No | `null` | Env var with ClickHouse username |
| `password_env_var` | No | `null` | Env var with ClickHouse password |
| `resource_key` | No | `null` | Key of a `ClickHouseResource`; when set, env var fields are ignored |
| `check_interval_seconds` | No | `300` | Seconds between observations |
| `default_status` | No | `running` | Sensor default status: `running` or `stopped` |

## Example YAML (env vars)

```yaml
type: dagster_component_templates.ClickHouseTableObservationSensorComponent
attributes:
  sensor_name: clickhouse_events_observation
  asset_key: clickhouse/analytics/events
  database: analytics
  table: events
  host_env_var: CLICKHOUSE_HOST
  password_env_var: CLICKHOUSE_PASSWORD
  check_interval_seconds: 300
```

## Example YAML (resource)

```yaml
type: dagster_component_templates.ClickHouseTableObservationSensorComponent
attributes:
  sensor_name: clickhouse_events_observation
  asset_key: clickhouse/analytics/events
  database: analytics
  table: events
  resource_key: clickhouse
```
