# Filesystem Monitor Sensor

Monitor local filesystem directories for new files and trigger jobs automatically.

## Overview

This sensor continuously monitors a specified directory for new files matching a pattern. When files are detected, it automatically triggers a job and passes file information via run_config to downstream assets.

Perfect for:
- Processing files as they arrive in a directory
- ETL pipelines that start with file ingestion
- Event-driven workflows based on file system events
- Automated data processing pipelines

## Features

- **Pattern Matching**: Use regex to filter specific file types
- **Recursive Monitoring**: Optionally search subdirectories
- **Run Config Support**: Passes file metadata to downstream assets
- **Deduplication**: Tracks processed files to avoid duplicates
- **Error Handling**: Gracefully handles missing directories and file access errors

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique name for this sensor
- **directory_path** (string) - Path to the directory to monitor
- **job_name** (string) - Name of the job to trigger

### Optional Parameters

- **file_pattern** (string) - Regex pattern to match files (default: `".*"` matches all files)
- **minimum_interval_seconds** (integer) - Time between checks in seconds (default: `30`)
- **recursive** (boolean) - Search subdirectories (default: `false`)
- **default_status** (string) - Initial sensor status: `running` or `stopped` (default: `"running"`)

## Usage

### Basic Example

Monitor a directory for CSV files:

```yaml
# File: defs/components/csv_sensor.yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: csv_files_sensor
  directory_path: /data/incoming
  file_pattern: ".*\\.csv$"
  job_name: process_csv_files
  minimum_interval_seconds: 60
```

### Recursive Monitoring

Monitor directory and all subdirectories:

```yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: recursive_file_sensor
  directory_path: /data/warehouse
  file_pattern: ".*\\.(csv|json|parquet)$"
  recursive: true
  job_name: ingest_all_files
```

### Multiple File Types

Monitor for different file formats:

```yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: multi_format_sensor
  directory_path: /data/exports
  file_pattern: "report_.*\\.(xlsx|csv|json)$"
  job_name: process_reports
```

## Run Config Schema

This sensor passes file information to downstream assets via run_config:

```python
{
  "ops": {
    "config": {
      "file_path": str,           # Full path to the file
      "file_name": str,           # Base filename
      "file_size": int,           # File size in bytes
      "file_modified_time": float, # Unix timestamp
      "directory_path": str       # Parent directory path
    }
  }
}
```

## Usage with Assets

### Create Asset That Accepts File Info

```yaml
# File: defs/components/file_processor.yaml
type: dagster_component_templates.FileTransformerAssetComponent
attributes:
  asset_name: process_incoming_files
  # This asset will receive file info from sensor
```

Or create a custom Python asset:

```python
from dagster import asset, Config, AssetExecutionContext

class FileConfig(Config):
    file_path: str
    file_name: str
    file_size: int
    file_modified_time: float
    directory_path: str

@asset
def process_file(context: AssetExecutionContext, config: FileConfig):
    context.log.info(f"Processing file: {config.file_path}")
    context.log.info(f"File size: {config.file_size} bytes")

    # Read and process the file
    with open(config.file_path, 'r') as f:
        data = f.read()
        # Process data...

    return data
```

### Create Job

```yaml
# File: defs/components/file_processing_job.yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: process_csv_files
  asset_selection: ["process_incoming_files"]
```

## Complete Example Pipeline

### 1. Filesystem Sensor

```yaml
# defs/components/data_files_sensor.yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: data_files_sensor
  directory_path: /data/incoming
  file_pattern: "data_.*\\.csv$"
  job_name: data_ingestion_job
  minimum_interval_seconds: 30
```

### 2. File Processing Asset

```yaml
# defs/components/ingest_file.yaml
type: dagster_component_templates.FileTransformerAssetComponent
attributes:
  asset_name: ingest_data_file
  output_format: parquet
  output_directory: /data/processed
```

### 3. Job

```yaml
# defs/components/data_ingestion_job.yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: data_ingestion_job
  asset_selection: ["ingest_data_file"]
```

### How It Works

1. **File Arrives**: New file `data_20240115.csv` appears in `/data/incoming`
2. **Sensor Detects**: Sensor checks directory every 30 seconds, finds new file
3. **Job Triggered**: Sensor creates RunRequest with file information
4. **Asset Processes**: Asset receives file path and processes the file
5. **Output Created**: Processed data saved to `/data/processed`

## Pattern Examples

### CSV Files Only
```yaml
file_pattern: ".*\\.csv$"
```

### JSON Files
```yaml
file_pattern: ".*\\.json$"
```

### Multiple Extensions
```yaml
file_pattern: ".*\\.(csv|json|txt)$"
```

### Date-Stamped Files
```yaml
file_pattern: "report_\\d{8}\\.csv$"  # Matches report_20240115.csv
```

### Files Starting with Prefix
```yaml
file_pattern: "^export_.*\\.xlsx$"  # Matches export_*.xlsx
```

## Directory Permissions

Ensure Dagster has read access to the monitored directory:

```bash
# Check permissions
ls -la /data/incoming

# Grant read access if needed
chmod 755 /data/incoming
```

## Troubleshooting

### Issue: "Directory does not exist"

**Solution**: Verify the directory path exists:
```bash
ls -la /data/incoming
```

Create the directory if needed:
```bash
mkdir -p /data/incoming
```

### Issue: "No new files found" (when you expect files)

**Solutions**:
1. Check the file pattern matches your files:
   ```bash
   ls /data/incoming | grep -E "your_pattern"
   ```

2. Verify sensor status is "running" in Dagster UI

3. Check minimum_interval_seconds - sensor may not have run yet

### Issue: Files processed multiple times

**Solution**: This sensor tracks file modification times to prevent reprocessing. If files are being modified after initial detection, they'll be processed again. Consider:
- Moving processed files to a different directory
- Using a downstream asset to track processed files
- Archiving files after processing

### Issue: Permission denied

**Solution**: Grant Dagster read permissions:
```bash
chmod 644 /data/incoming/*.csv
```

## Performance Considerations

1. **Large Directories**: For directories with thousands of files, use more specific file_pattern to reduce scan time

2. **Frequent Changes**: Increase minimum_interval_seconds for directories that change very frequently

3. **Deep Directory Trees**: Set recursive=false if you don't need subdirectory scanning

4. **File Processing Time**: Ensure your job can complete before the next sensor evaluation

## Integration Examples

### With S3 Upload

Monitor local directory, then upload to S3:

```python
@asset
def upload_to_s3(context: AssetExecutionContext, config: FileConfig):
    import boto3
    s3 = boto3.client('s3')
    s3.upload_file(config.file_path, 'my-bucket', config.file_name)
```

### With Database Loading

Monitor for CSV files, load into database:

```python
@asset
def load_to_database(context: AssetExecutionContext, config: FileConfig):
    import pandas as pd
    from sqlalchemy import create_engine

    df = pd.read_csv(config.file_path)
    engine = create_engine(context.resources.database_url)
    df.to_sql('data_table', engine, if_exists='append')
```

### With Data Validation

Monitor files and validate before processing:

```python
@asset
def validate_and_process(context: AssetExecutionContext, config: FileConfig):
    import pandas as pd

    df = pd.read_csv(config.file_path)

    # Validate
    assert len(df) > 0, "Empty file"
    assert 'id' in df.columns, "Missing id column"

    # Process
    # ...
```

## Monitoring

The sensor provides metadata for each run:
- File path and name
- File size
- Modification timestamp
- Directory path

Access this information in:
- Dagster UI under sensor evaluations
- Asset materialization metadata
- Run logs

## Best Practices

1. **Use Specific Patterns**: More specific file_pattern reduces false positives
2. **Set Appropriate Intervals**: Balance between responsiveness and system load
3. **Handle Partial Files**: Consider checking file stability before processing
4. **Archive Processed Files**: Move files after processing to keep directory clean
5. **Monitor Disk Space**: Ensure adequate space for incoming files
6. **Use Subdirectories**: Organize files by date or type for better management

## Requirements

- Python 3.8+
- Dagster 1.5.0+
- Read permissions on monitored directory

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
