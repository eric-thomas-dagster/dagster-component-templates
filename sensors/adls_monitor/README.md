# ADLS Monitor Sensor

Monitor an Azure Data Lake Storage Gen2 container for new files and trigger jobs automatically.

## Overview

This sensor continuously monitors an ADLS Gen2 container directory for new files matching a pattern. When files are detected, it automatically triggers a job and passes file metadata via run_config to downstream assets.

Perfect for:
- Processing files as they arrive in Azure Data Lake Storage
- Event-driven ETL pipelines starting from ADLS ingestion
- Triggering workflows when partners drop files in a shared container
- Automated data processing for Azure-based data lakes

## Features

- **Directory Scoping**: Monitor a specific path within a container
- **Pattern Matching**: Use regex to filter specific file names
- **Recursive Listing**: Optionally list files across all subdirectories
- **Run Config Support**: Passes file metadata to downstream assets
- **Deduplication**: Tracks processed files using last-modified timestamps
- **Flexible Auth**: Supports DefaultAzureCredential (managed identity, service principal, CLI) and connection strings
- **Error Handling**: Gracefully handles ADLS connectivity and permission errors

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique name for this sensor
- **storage_account_name** (string) - Azure Storage account name
- **container_name** (string) - ADLS Gen2 container (filesystem) name
- **job_name** (string) - Name of the job to trigger

### Optional Parameters

- **directory_path** (string) - Directory path within the container (default: `""` monitors container root)
- **file_pattern** (string) - Regex pattern matched against file names (default: `".*"` matches all files)
- **minimum_interval_seconds** (integer) - Time between checks in seconds (default: `30`)
- **recursive** (boolean) - List files in subdirectories (default: `false`)
- **connection_string_env_var** (string) - Environment variable name holding the connection string (default: uses `DefaultAzureCredential`)
- **default_status** (string) - Initial sensor status: `running` or `stopped` (default: `"running"`)

## Authentication

### DefaultAzureCredential (Recommended)

By default, this sensor uses `DefaultAzureCredential`, which tries the following in order:

1. `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET` environment variables
2. Workload identity (AKS)
3. Managed identity (Azure VMs, Azure Functions, etc.)
4. `az login` (Azure CLI)
5. Visual Studio Code credentials

No explicit configuration is needed when running on Azure infrastructure with a managed identity.

### Connection String

For non-Azure environments or local development, set the connection string in an environment variable and reference it:

```yaml
type: dagster_component_templates.ADLSMonitorSensorComponent
attributes:
  sensor_name: adls_sensor
  storage_account_name: mydatalake
  container_name: raw
  job_name: process_files
  connection_string_env_var: AZURE_STORAGE_CONNECTION_STRING
```

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
```

### Required RBAC Role

Assign the **Storage Blob Data Reader** role (or higher) to the identity used by this sensor on the target storage account or container.

## Usage

### Basic Example

Monitor a container directory for Parquet files:

```yaml
# File: defs/components/adls_sensor.yaml
type: dagster_component_templates.ADLSMonitorSensorComponent
attributes:
  sensor_name: adls_parquet_sensor
  storage_account_name: mydatalake
  container_name: raw
  directory_path: incoming/
  file_pattern: ".*\\.parquet$"
  job_name: process_parquet_files
  minimum_interval_seconds: 60
```

### Recursive Monitoring

Monitor a directory and all subdirectories:

```yaml
type: dagster_component_templates.ADLSMonitorSensorComponent
attributes:
  sensor_name: adls_recursive_sensor
  storage_account_name: mydatalake
  container_name: data-warehouse
  directory_path: exports/
  file_pattern: ".*\\.(csv|json|parquet)$"
  recursive: true
  job_name: ingest_all_exports
```

### Monitor Multiple File Types

```yaml
type: dagster_component_templates.ADLSMonitorSensorComponent
attributes:
  sensor_name: adls_multi_format_sensor
  storage_account_name: analytics
  container_name: landing
  file_pattern: ".*\\.(csv|json|parquet|avro)$"
  job_name: ingest_files
  minimum_interval_seconds: 120
```

## Run Config Schema

This sensor passes ADLS file information to downstream assets via run_config:

```python
{
  "ops": {
    "config": {
      "storage_account": str,  # Azure Storage account name
      "container": str,        # Container (filesystem) name
      "file_path": str,        # Full path within the container
      "file_name": str,        # Base file name
      "size": int,             # File size in bytes
      "last_modified": str,    # ISO 8601 timestamp
      "directory_path": str    # Directory path used for monitoring
    }
  }
}
```

## Usage with Assets

### Create Asset That Accepts ADLS File Info

```python
from dagster import asset, Config, AssetExecutionContext

class ADLSFileConfig(Config):
    storage_account: str
    container: str
    file_path: str
    file_name: str
    size: int
    last_modified: str
    directory_path: str

@asset
def process_adls_file(context: AssetExecutionContext, config: ADLSFileConfig):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{config.storage_account}.dfs.core.windows.net",
        credential=credential,
    )
    fs_client = service_client.get_file_system_client(config.container)
    file_client = fs_client.get_file_client(config.file_path)

    download = file_client.download_file()
    data = download.readall()

    context.log.info(f"Processed {config.file_name} ({config.size} bytes)")
    return data
```

### Read Parquet with pandas

```python
@asset
def load_parquet_from_adls(context: AssetExecutionContext, config: ADLSFileConfig):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    import pandas as pd
    import io

    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{config.storage_account}.dfs.core.windows.net",
        credential=credential,
    )
    fs_client = service_client.get_file_system_client(config.container)
    file_client = fs_client.get_file_client(config.file_path)

    download = file_client.download_file()
    df = pd.read_parquet(io.BytesIO(download.readall()))

    context.log.info(f"Loaded {len(df)} rows from {config.file_path}")
    return df
```

## Complete Example Pipeline

### 1. ADLS Sensor

```yaml
# defs/components/adls_data_sensor.yaml
type: dagster_component_templates.ADLSMonitorSensorComponent
attributes:
  sensor_name: adls_data_sensor
  storage_account_name: mydatalake
  container_name: raw
  directory_path: incoming/csv/
  file_pattern: ".*\\.csv$"
  job_name: csv_ingestion_job
  minimum_interval_seconds: 60
```

### How It Works

1. **File Arrives**: New file `data_20240115.csv` is uploaded to `raw/incoming/csv/`
2. **Sensor Detects**: Sensor lists files every 60 seconds, finds files newer than cursor
3. **Job Triggered**: Sensor creates RunRequest with file metadata
4. **Asset Processes**: Asset downloads and processes the file
5. **Cursor Updated**: Sensor stores the latest `last_modified` timestamp as the new cursor

## Pattern Examples

### CSV Files Only
```yaml
file_pattern: ".*\\.csv$"
```

### Parquet Files
```yaml
file_pattern: ".*\\.parquet$"
```

### Multiple Extensions
```yaml
file_pattern: ".*\\.(csv|json|parquet|avro)$"
```

### Date-Stamped Files
```yaml
file_pattern: "data_\\d{8}\\.csv$"  # Matches data_20240115.csv
```

## Troubleshooting

### Issue: "Failed to create ADLS client"

**Solutions**:
1. Verify `DefaultAzureCredential` can resolve: `az account show`
2. Check environment variables: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`
3. If using connection string, confirm the env var is set correctly

### Issue: "Error listing ADLS files"

**Solutions**:
1. Verify storage account name and container name are correct
2. Confirm the identity has **Storage Blob Data Reader** role
3. Check that the `directory_path` exists in the container

### Issue: "No new ADLS files found" (when you expect files)

**Solutions**:
1. Verify the file pattern matches your file names
2. Check sensor status is "running" in Dagster UI
3. Reset the sensor cursor in the Dagster UI if it has advanced past existing files

## Performance Considerations

1. **Large Containers**: Use specific `directory_path` values to reduce the number of files listed
2. **Frequent Uploads**: Increase `minimum_interval_seconds` for high-volume containers
3. **Deep Hierarchies**: Avoid `recursive: true` on very deep directory trees unless necessary

## Requirements

- Python 3.8+
- Dagster 1.5.0+
- azure-storage-file-datalake>=12.0.0
- azure-identity>=1.12.0
- Azure identity with Storage Blob Data Reader role on the target container

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
