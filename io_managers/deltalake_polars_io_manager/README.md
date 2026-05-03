# DeltaLakePolarsIOManager

Polars variant of delta_lake_io_manager. Wraps the official `dagster-deltalake-polars` package — Polars DataFrames written/read as Delta Lake tables.

Wraps the official `dagster-deltalake-polars` package.

## Example

```yaml
type: dagster_component_templates.DeltaLakePolarsIOManagerComponent
attributes:
  resource_key: <fill in>
  root_uri: <fill in>
  schema_name: <fill in>
```

## Requirements

```
dagster
dagster-deltalake-polars
polars
```
