# S3CsvIOManager

ConfigurableIOManager that writes pandas DataFrames as CSV to S3. Slower / lossier than the Parquet variant but readable from any tool. Supports partitioned assets and S3-compatible endpoints (MinIO, LocalStack).

## Example

```yaml
type: dagster_component_templates.S3CsvIOManagerComponent
attributes:
  resource_key: io_manager
  bucket: <fill in>
  prefix: <fill in>
  region_name: <fill in>
  aws_access_key_env_var: <fill in>
  aws_secret_key_env_var: <fill in>
  endpoint_url: <fill in>
```

## Requirements

```
pandas
boto3
```
