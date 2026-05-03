# ClickHouseIOManager

ConfigurableIOManager that writes pandas DataFrames to ClickHouse via clickhouse-connect. Asset key becomes the table name; default engine is MergeTree.

## Example

```yaml
type: dagster_component_templates.ClickHouseIOManagerComponent
attributes:
  resource_key: io_manager
  host: <fill in>
  port: <fill in>
  username_env_var: <fill in>
  password_env_var: <fill in>
  database: <fill in>
  secure: <fill in>
```

## Requirements

```
pandas
clickhouse-connect
```
