# Snowflake Stored Procedure — Call Asset

Materializes by calling `CALL <db>.<schema>.<proc>(args)` on a named existing Snowflake stored procedure. Single-entity counterpart to the auto-discovered stored-procedure assets in `snowflake_workspace`.

## Example

```yaml
type: dagster_community_components.SnowflakeStoredProcedureCallAssetComponent
attributes:
  asset_key: snowflake/procs/transform_customers
  procedure_name: TRANSFORM_CUSTOMERS
  database: DAGSTER_DEMO
  schema: ANALYTICS
  arguments: []
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
```

### With arguments

Pass positional arguments to the proc. Strings are auto-quoted; numbers / booleans are inlined:

```yaml
attributes:
  procedure_name: REFRESH_BY_PARTITION
  arguments:
    - "2026-05-22"     # quoted as a string literal
    - 100              # inlined as a number
    - true             # inlined as TRUE
```

## Auth options

Same as `snowflake_task_execute_asset` — password / PAT / keypair via `*_env_var` fields.

## Requirements

```
snowflake-connector-python>=3.0.0
```

## See also

- [`snowflake_workspace`](https://dagster-component-ui.vercel.app/c/snowflake_workspace) — multi-entity auto-discovery
- [`snowflake_task_execute_asset`](https://dagster-community-components-cli/c/snowflake_task_execute_asset) — sibling for tasks
