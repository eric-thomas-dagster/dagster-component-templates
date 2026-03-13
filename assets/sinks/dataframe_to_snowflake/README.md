# DataFrame to Snowflake

Write a pandas DataFrame to a Snowflake table using the native Snowflake Connector for Python.

## Overview

This component receives a DataFrame from an upstream Dagster asset and writes it to a Snowflake table. The table can be created automatically if it does not exist. Column names are automatically uppercased to comply with Snowflake conventions.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `table` | `str` | required | Destination Snowflake table name |
| `database` | `Optional[str]` | `None` | Snowflake database (overrides connection default) |
| `schema` | `Optional[str]` | `None` | Snowflake schema |
| `warehouse` | `Optional[str]` | `None` | Snowflake virtual warehouse to use |
| `role` | `Optional[str]` | `None` | Snowflake role to assume |
| `account_env_var` | `str` | `"SNOWFLAKE_ACCOUNT"` | Env var containing the Snowflake account identifier |
| `user_env_var` | `str` | `"SNOWFLAKE_USER"` | Env var containing the Snowflake user |
| `password_env_var` | `str` | `"SNOWFLAKE_PASSWORD"` | Env var containing the Snowflake password |
| `if_exists` | `str` | `"replace"` | Behavior when the table exists: `replace`, `append`, or `fail` |
| `chunksize` | `Optional[int]` | `None` | Number of rows per write chunk (None = connector default) |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToSnowflakeComponent
attributes:
  asset_name: snowflake_orders
  upstream_asset_key: processed_orders
  table: orders
  database: ANALYTICS
  schema: PUBLIC
  warehouse: COMPUTE_WH
  role: TRANSFORMER
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  if_exists: replace
  chunksize: 10000
  group_name: warehouse_sinks
```

## Authentication / Credentials

Set the following environment variables before running:

```bash
export SNOWFLAKE_ACCOUNT="myorg-myaccount"   # e.g. xy12345.us-east-1
export SNOWFLAKE_USER="my_service_user"
export SNOWFLAKE_PASSWORD="supersecret"
```

You can override the env var names using `account_env_var`, `user_env_var`, and `password_env_var` fields if your environment uses different variable names.

## if_exists Options

- `replace` — drops the existing table (if present) and recreates it from the DataFrame schema before writing.
- `append` — inserts rows into the existing table without removing prior data.
- `fail` — raises an error if the table already contains data.

## Column Name Behaviour

Snowflake requires unquoted identifiers to be uppercase. This component automatically converts all DataFrame column names to uppercase before writing. Ensure your downstream Snowflake queries use uppercase column references or quoted identifiers.

## Requirements

```
dagster
pandas
snowflake-connector-python[pandas]
```

Install with:

```bash
pip install dagster pandas "snowflake-connector-python[pandas]"
```
