# SnowflakePySparkIOManager

PySpark variant of snowflake_io_manager. Wraps the official `dagster-snowflake-pyspark` package.

Wraps the official `dagster-snowflake-pyspark` package.

## Example

```yaml
type: dagster_component_templates.SnowflakePySparkIOManagerComponent
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
dagster-snowflake-pyspark
pyspark
```
