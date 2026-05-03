# ClickhousePolarsIOManager

Polars variant of clickhouse_io_manager. Same backend (ClickHouse) but with the Polars query engine — much faster on wide rows. Wraps the official `dagster-clickhouse-polars` package.

Wraps the official `dagster-clickhouse-polars` package.

## Example

```yaml
type: dagster_component_templates.ClickhousePolarsIOManagerComponent
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
dagster-clickhouse-polars
polars
```
