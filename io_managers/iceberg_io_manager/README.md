# IcebergIOManager

ConfigurableIOManager that wraps the official `dagster-iceberg` package's IcebergPyarrowIOManager. Supports REST, Hive, and Glue catalogs via the standard Iceberg catalog config. Each asset becomes a namespaced Iceberg table; partitioned assets get one Iceberg-partition per Dagster-partition.

This component **wraps the official `dagster-iceberg` package** rather than reimplementing the IO manager — gets us format compatibility, schema evolution, partition handling, etc. for free.

## Example

```yaml
type: dagster_component_templates.IcebergIOManagerComponent
attributes:
  resource_key: io_manager
  catalog_name: <fill in>
  namespace: <fill in>
  catalog_uri: <fill in>
  warehouse: <fill in>
```

## Requirements

```
dagster
dagster-iceberg
pyarrow
```
