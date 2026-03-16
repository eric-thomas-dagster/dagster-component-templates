# DuckDB IO Manager

Register a `DuckDBPandasIOManager` so assets are automatically stored in and loaded from a local DuckDB file — wraps the official `dagster-duckdb-pandas` integration.

## Installation

```
pip install dagster-duckdb-pandas
```

## Configuration

```yaml
type: dagster_component_templates.DuckDBIOManagerComponent
attributes:
  resource_key: io_manager
  database: data/warehouse.duckdb
  schema_name: main
```

For MotherDuck (serverless DuckDB cloud), use the `motherduck_io_manager` component instead.
