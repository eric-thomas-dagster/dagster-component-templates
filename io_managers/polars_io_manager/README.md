# PolarsIOManager

ConfigurableIOManager that wraps the official `dagster-polars` package's PolarsParquetIOManager. Reads/writes polars.DataFrame as Parquet. Same disk layout as a local-Parquet IO manager but with Polars' query engine — much faster on large rows / wide tables than pandas.

This component **wraps the official `dagster-polars` package** rather than reimplementing the IO manager — gets us format compatibility, schema evolution, partition handling, etc. for free.

## Example

```yaml
type: dagster_component_templates.PolarsIOManagerComponent
attributes:
  resource_key: io_manager
  base_dir: <fill in>
```

## Requirements

```
dagster
dagster-polars
polars
pyarrow
```
