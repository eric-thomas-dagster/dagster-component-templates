# SnowflakePolarsIOManager

Polars variant of snowflake_io_manager. Wraps the official `dagster-snowflake-polars` package — DataFrames are Polars instead of pandas, with the speed advantages on wide rows / high cardinality.

Wraps the official `dagster-snowflake-polars` package.

## Example

```yaml
type: dagster_component_templates.SnowflakePolarsIOManagerComponent
attributes:
  resource_key: <fill in>
  account: <fill in>
  user_env_var: <fill in>
  password_env_var: <fill in>
  database: <fill in>
  schema_name: <fill in>
  warehouse: <fill in>
  role: <fill in>
```

## Requirements

```
dagster
dagster-snowflake-polars
polars
```
