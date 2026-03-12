# External ClickHouse Table

Declares a ClickHouse table as an observable external asset in Dagster. Use alongside `clickhouse_table_observation_sensor` for continuous health monitoring of row counts, disk size, active parts, and engine type.

## Required packages

```
clickhouse-connect>=0.6.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_key` | Yes | — | Dagster asset key (e.g. `clickhouse/analytics/events`) |
| `database` | Yes | — | ClickHouse database name |
| `table` | Yes | — | ClickHouse table name |
| `host_env_var` | Yes | — | Env var containing the ClickHouse host |
| `port` | No | `8443` | ClickHouse port (8443 for HTTPS, 8123 for HTTP) |
| `username_env_var` | No | `null` | Env var containing the ClickHouse username |
| `password_env_var` | No | `null` | Env var containing the ClickHouse password |
| `description` | No | `null` | Human-readable description |
| `group_name` | No | `clickhouse` | Dagster asset group name |
| `owners` | No | `null` | List of owner emails or team names |

## Example YAML

```yaml
type: dagster_component_templates.ExternalClickHouseTableComponent
attributes:
  asset_key: clickhouse/analytics/events
  database: analytics
  table: events
  host_env_var: CLICKHOUSE_HOST
  password_env_var: CLICKHOUSE_PASSWORD
  group_name: clickhouse
```
