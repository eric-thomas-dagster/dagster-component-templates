# PolarsIOManager

ConfigurableIOManager that reads/writes polars.DataFrame as Parquet. Same disk layout as local_parquet_io_manager but with the Polars query engine — much faster on large rows / wide tables.

## Example

```yaml
type: dagster_component_templates.PolarsIOManagerComponent
attributes:
  resource_key: io_manager
  base_dir: <fill in>
```

## Requirements

```
polars
pyarrow
```
