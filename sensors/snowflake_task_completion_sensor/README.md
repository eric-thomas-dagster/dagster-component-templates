# Snowflake Task Completion Sensor

Polls Snowflake `TASK_HISTORY` for a named task and emits `AssetMaterialization` on each new SUCCESS row. Optionally fires a downstream Dagster job via `RunRequest`.

## Why this exists

A Snowflake task can be triggered three ways:

1. **`EXECUTE TASK`** via `snowflake_task_execute_asset` — Dagster materializes the asset
2. **Snowflake's own cron schedule** — Snowflake fires the task autonomously
3. **As a child task** in a task tree — parent fires it

`snowflake_task_execute_asset` only records (1). This sensor records (2) and (3) — by polling `TASK_HISTORY`, the sensor sees every SUCCESS regardless of trigger source.

## Three operating modes

Configure via the `asset_key` and `job_name` fields:

| `asset_key` | `job_name` | Behavior |
|---|---|---|
| set | unset | **Observe-only** — emit AssetMaterialization on each SUCCESS, no downstream trigger |
| unset | set | **Trigger-only** — fire RunRequest on each SUCCESS, no catalog update |
| set | set | **Both** — record + trigger |

## Example

```yaml
type: dagster_community_components.SnowflakeTaskCompletionSensorComponent
attributes:
  sensor_name: refresh_orders_done
  task_name: DAILY_REFRESH_ORDERS
  database: DAGSTER_DEMO
  schema: RAW
  asset_key: snowflake/tasks/refresh_orders      # pair with execute asset
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  minimum_interval_seconds: 60
  default_status: stopped
```

## Cursor + dedup

The sensor uses the **most-recent `query_id`** as its cursor. It stops processing when it hits a query_id it's already seen, so the same SUCCESS row never fires twice. Each `RunRequest` is keyed by `query_id` for Dagster's own dedup as a safety net.

## Auth options

Same as `snowflake_task_execute_asset` — password / PAT / keypair via `*_env_var` fields.

## Requirements

```
snowflake-connector-python>=3.0.0
```
