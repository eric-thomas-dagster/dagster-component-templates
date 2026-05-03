# AzureBlobParquetIOManager

ConfigurableIOManager that writes pandas DataFrames as Parquet to an Azure Blob Storage container. Asset path becomes the blob path; supports partitioned assets.

## Example

```yaml
type: dagster_component_templates.AzureBlobParquetIOManagerComponent
attributes:
  resource_key: io_manager
  account_name: <fill in>
  container: <fill in>
  prefix: <fill in>
  connection_string_env_var: <fill in>
```

## Requirements

```
pandas
pyarrow
azure-storage-blob
azure-identity
```
