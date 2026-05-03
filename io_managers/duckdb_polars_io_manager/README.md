# DuckDBPolarsIOManager

Polars variant of duckdb_io_manager. Same backend (DuckDB) but Polars instead of pandas. Wraps the official `dagster-duckdb-polars` package.

Wraps the official `dagster-duckdb-polars` package.

## Example

```yaml
type: dagster_component_templates.DuckDBPolarsIOManagerComponent
attributes:
  resource_key: <fill in>
  database: <fill in>
  schema_name: <fill in>
```

## Requirements

```
dagster
dagster-duckdb-polars
polars
```
