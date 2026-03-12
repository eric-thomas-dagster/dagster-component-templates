# S3 Monitor Sensor

Monitor an Amazon S3 bucket prefix for new objects and trigger jobs automatically.

## Overview

This sensor continuously monitors an S3 bucket prefix for new objects matching a key pattern. When objects are detected, it automatically triggers a job and passes object metadata via run_config to downstream assets.

Perfect for:
- Processing files as they arrive in S3
- Event-driven ETL pipelines starting from S3 ingestion
- Triggering workflows when partners drop files in a shared bucket
- Automated data processing for S3-based data lakes

## Features

- **Prefix Scoping**: Monitor a specific path within a bucket
- **Pattern Matching**: Use regex to filter specific object keys
- **Run Config Support**: Passes object metadata to downstream assets
- **Deduplication**: Tracks processed objects using modification timestamps and ETags
- **Pagination**: Handles buckets with thousands of objects via automatic pagination
- **Error Handling**: Gracefully handles S3 connectivity and permission errors

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique name for this sensor
- **bucket_name** (string) - Name of the S3 bucket to monitor
- **job_name** (string) - Name of the job to trigger

### Optional Parameters

- **prefix** (string) - S3 key prefix to scope monitoring (default: `""` monitors entire bucket)
- **key_pattern** (string) - Regex pattern matched against the key relative to the prefix (default: `".*"` matches all objects)
- **minimum_interval_seconds** (integer) - Time between checks in seconds (default: `30`)
- **region_name** (string) - AWS region (default: uses boto3 default region from environment)
- **default_status** (string) - Initial sensor status: `running` or `stopped` (default: `"running"`)

## Authentication

This sensor uses boto3 for AWS authentication. Credentials are resolved in the standard boto3 order:

1. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile (when running on EC2, ECS, Lambda, etc.)

No credentials need to be explicitly configured in this component.

### Required IAM Permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket",
    "s3:GetObject"
  ],
  "Resource": [
    "arn:aws:s3:::my-data-bucket",
    "arn:aws:s3:::my-data-bucket/*"
  ]
}
```

## Usage

### Basic Example

Monitor a bucket prefix for Parquet files:

```yaml
# File: defs/components/s3_sensor.yaml
type: dagster_component_templates.S3MonitorSensorComponent
attributes:
  sensor_name: s3_parquet_sensor
  bucket_name: my-data-bucket
  prefix: incoming/
  key_pattern: ".*\\.parquet$"
  job_name: process_parquet_files
  minimum_interval_seconds: 60
```

### Monitor Multiple File Types

```yaml
type: dagster_component_templates.S3MonitorSensorComponent
attributes:
  sensor_name: s3_multi_format_sensor
  bucket_name: analytics-bucket
  prefix: exports/
  key_pattern: ".*\\.(csv|json|parquet)$"
  job_name: ingest_exported_files
```

### Date-Partitioned Prefix

```yaml
type: dagster_component_templates.S3MonitorSensorComponent
attributes:
  sensor_name: s3_daily_sensor
  bucket_name: data-lake
  prefix: raw/events/
  key_pattern: "\\d{4}-\\d{2}-\\d{2}/.*\\.json\\.gz$"
  job_name: process_daily_events
  minimum_interval_seconds: 300
```

### Monitor Entire Bucket

```yaml
type: dagster_component_templates.S3MonitorSensorComponent
attributes:
  sensor_name: s3_bucket_sensor
  bucket_name: partner-drop-zone
  job_name: process_partner_files
  minimum_interval_seconds: 120
```

## Run Config Schema

This sensor passes S3 object information to downstream assets via run_config:

```python
{
  "ops": {
    "config": {
      "bucket": str,         # S3 bucket name
      "key": str,            # Full S3 object key
      "size": int,           # Object size in bytes
      "etag": str,           # Object ETag (MD5 hash for non-multipart uploads)
      "last_modified": str,  # ISO 8601 timestamp
      "prefix": str,         # Prefix used for monitoring
      "region": str          # AWS region (empty string if using default)
    }
  }
}
```

## Usage with Assets

### Create Asset That Accepts S3 Object Info

```python
from dagster import asset, Config, AssetExecutionContext

class S3ObjectConfig(Config):
    bucket: str
    key: str
    size: int
    etag: str
    last_modified: str
    prefix: str
    region: str

@asset
def process_s3_object(context: AssetExecutionContext, config: S3ObjectConfig):
    import boto3
    s3 = boto3.client("s3")

    context.log.info(f"Processing s3://{config.bucket}/{config.key} ({config.size} bytes)")

    obj = s3.get_object(Bucket=config.bucket, Key=config.key)
    data = obj["Body"].read()

    # Process data...
    return data
```

### Read Parquet Directly

```python
@asset
def load_parquet_from_s3(context: AssetExecutionContext, config: S3ObjectConfig):
    import boto3
    import pandas as pd
    import io

    s3 = boto3.client("s3", region_name=config.region or None)
    obj = s3.get_object(Bucket=config.bucket, Key=config.key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    context.log.info(f"Loaded {len(df)} rows from s3://{config.bucket}/{config.key}")
    return df
```

## Complete Example Pipeline

### 1. S3 Sensor

```yaml
# defs/components/s3_data_sensor.yaml
type: dagster_component_templates.S3MonitorSensorComponent
attributes:
  sensor_name: s3_data_sensor
  bucket_name: my-data-lake
  prefix: raw/csv/
  key_pattern: ".*\\.csv$"
  job_name: csv_ingestion_job
  minimum_interval_seconds: 60
  region_name: us-east-1
```

### 2. Processing Asset

```python
# defs/assets/ingest_csv.py
from dagster import asset, Config, AssetExecutionContext
import boto3
import pandas as pd
import io

class S3ObjectConfig(Config):
    bucket: str
    key: str
    size: int
    etag: str
    last_modified: str
    prefix: str
    region: str

@asset
def ingest_csv_from_s3(context: AssetExecutionContext, config: S3ObjectConfig):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=config.bucket, Key=config.key)
    df = pd.read_csv(io.StringIO(obj["Body"].read().decode("utf-8")))
    context.log.info(f"Ingested {len(df)} rows from {config.key}")
    return df
```

### How It Works

1. **Object Arrives**: New file `data_20240115.csv` is uploaded to `s3://my-data-lake/raw/csv/`
2. **Sensor Detects**: Sensor lists objects every 60 seconds, finds new object newer than cursor
3. **Job Triggered**: Sensor creates RunRequest with object metadata
4. **Asset Processes**: Asset downloads and processes the file
5. **Cursor Updated**: Sensor stores the object's `LastModified` timestamp as the new cursor

## Pattern Examples

### CSV Files Only
```yaml
key_pattern: ".*\\.csv$"
```

### Gzipped JSON
```yaml
key_pattern: ".*\\.json\\.gz$"
```

### Parquet in Date Partitions
```yaml
key_pattern: "\\d{4}/\\d{2}/\\d{2}/.*\\.parquet$"
```

### Files with Specific Prefix in Key
```yaml
key_pattern: "^export_.*\\.xlsx$"
```

### Avro or ORC Files
```yaml
key_pattern: ".*\\.(avro|orc)$"
```

## Troubleshooting

### Issue: "Failed to create S3 client"

**Solution**: Verify AWS credentials are configured:
```bash
aws sts get-caller-identity
```

### Issue: "Error listing S3 objects"

**Solutions**:
1. Check bucket name and region are correct
2. Verify IAM permissions include `s3:ListBucket`
3. Confirm the prefix exists in the bucket

### Issue: "No new S3 objects found" (when you expect objects)

**Solutions**:
1. Verify the key pattern matches your object keys:
   ```bash
   aws s3 ls s3://my-bucket/incoming/ | grep -E "your_pattern"
   ```
2. Check sensor status is "running" in Dagster UI
3. The cursor may be ahead of existing objects — reset it in the Dagster UI

### Issue: Objects processed multiple times

**Solution**: This sensor uses `run_key` combining the object key and ETag, which prevents duplicate runs. If objects are being replaced (same key, new content), the ETag will change and the new version will be processed.

## Performance Considerations

1. **Large Buckets**: Use specific prefixes to reduce the number of objects listed per evaluation
2. **Frequent Uploads**: Increase `minimum_interval_seconds` for high-volume buckets
3. **Pagination**: The sensor automatically paginates through all matching objects — very large result sets increase evaluation time
4. **Avoid Root Prefix**: Always specify a prefix when possible to reduce list overhead

## Requirements

- Python 3.8+
- Dagster 1.5.0+
- boto3>=1.26.0
- AWS credentials with `s3:ListBucket` and `s3:GetObject` permissions

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
