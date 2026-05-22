# Snowflake Task — Execute Asset

Materializes by calling `EXECUTE TASK <db>.<schema>.<task_name>` on a named existing Snowflake task. Pulls the most recent `TASK_HISTORY` entry into the materialization metadata.

Single-entity counterpart to the multi-entity `snowflake_workspace` component — use this when you want to wire one specific task into a Dagster project (without scanning the whole account for tasks).

## Example

```yaml
type: dagster_community_components.SnowflakeTaskExecuteAssetComponent
attributes:
  asset_key: snowflake/tasks/refresh_orders
  task_name: DAILY_REFRESH_ORDERS
  database: DAGSTER_DEMO
  schema: RAW
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  warehouse_env_var: SNOWFLAKE_WAREHOUSE
  role_env_var: SNOWFLAKE_ROLE
```

## Pairing with `snowflake_task_completion_sensor`

By default the asset materializes only when **you** trigger it (e.g. `dg launch --assets ...`). The Snowflake scheduler also fires the task on its own cron — to surface THOSE runs in Dagster's catalog too, pair with `snowflake_task_completion_sensor`:

```yaml
type: dagster_community_components.SnowflakeTaskCompletionSensorComponent
attributes:
  sensor_name: refresh_orders_task_done
  task_name: DAILY_REFRESH_ORDERS
  database: DAGSTER_DEMO
  schema: RAW
  asset_key: snowflake/tasks/refresh_orders   # same as the asset above
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
```

The sensor watches `TASK_HISTORY` for new SUCCESS rows and emits `AssetMaterialization` for the same asset_key.

## Auth options

| Method | Set these env vars |
|---|---|
| Password | `account_env_var`, `user_env_var`, `password_env_var` |
| Programmatic Access Token (PAT) | `account_env_var`, `user_env_var`, `pat_env_var` |
| Keypair (JWT) | `account_env_var`, `user_env_var`, `private_key_path_env_var`, optional `private_key_passphrase_env_var` |

## Requirements

```
snowflake-connector-python>=3.0.0
```

## See also

- [`snowflake_workspace`](https://dagster-component-ui.vercel.app/c/snowflake_workspace) — multi-entity auto-discovery (tasks + procs + dynamic tables + streams + pipes + …)
- [`snowflake_task_completion_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_task_completion_sensor) — surface Snowflake-scheduler-fired runs in the catalog
