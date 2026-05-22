# Snowflake Dynamic Table — Refresh Asset

Materializes by calling `ALTER DYNAMIC TABLE <db>.<schema>.<name> REFRESH` on a named existing Snowflake dynamic table. Pulls scheduling state + last-refresh-status into materialization metadata.

Single-entity counterpart to the auto-discovered dynamic-table assets in `snowflake_workspace`.

## Example

```yaml
type: dagster_community_components.SnowflakeDynamicTableRefreshAssetComponent
attributes:
  asset_key: snowflake/dt/customer_summary
  dynamic_table_name: CUSTOMER_SUMMARY
  database: DAGSTER_DEMO
  schema: ANALYTICS
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
```

## Materialization metadata

| Key | Description |
|---|---|
| `dynamic_table_fqn` | `<db>.<schema>.<name>` |
| `scheduling_state` | RUNNING / SUSPENDED / SUSPENDED_ON_ERROR |
| `last_refresh_status` | SUCCESS / FAILED / CANCELED |
| `target_lag` | Configured lag target |
| `refresh_mode` | AUTO / FULL / INCREMENTAL |
| `last_successful_refresh_time` | ISO timestamp |

## Auth options

Same as `snowflake_task_execute_asset` — password / PAT / keypair.

## Requirements

```
snowflake-connector-python>=3.0.0
```
