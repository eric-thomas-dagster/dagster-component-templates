# ClickhouseIOManager

ConfigurableIOManager wrapping the official `dagster-clickhouse-pandas` package. Each asset becomes a ClickHouse table; partitioned assets get partitioned tables. Uses the maintained upstream package for type handling, partitions, and schema evolution.

Wraps the official `dagster-clickhouse-pandas` package.

## Example

```yaml
type: dagster_component_templates.ClickhouseIOManagerComponent
attributes:
  resource_key: <fill in>
  host: <fill in>
  port: <fill in>
  username_env_var: <fill in>
  password_env_var: <fill in>
  database: <fill in>
  secure: <fill in>
```

## Requirements

```
dagster
dagster-clickhouse-pandas
```
