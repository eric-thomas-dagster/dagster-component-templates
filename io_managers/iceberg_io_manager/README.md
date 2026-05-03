# IcebergIOManager

ConfigurableIOManager that writes pandas DataFrames to Iceberg tables. Supports REST, Hive, and Glue catalogs. Each asset becomes a namespaced Iceberg table; partitioned assets get one Iceberg-partition per Dagster-partition.

## Example

```yaml
type: dagster_component_templates.IcebergIOManagerComponent
attributes:
  resource_key: io_manager
  catalog_name: <fill in>
  namespace: <fill in>
  catalog_uri: <fill in>
  warehouse: <fill in>
  catalog_type: <fill in>
```

## Requirements

```
pandas
pyiceberg
pyarrow
```
