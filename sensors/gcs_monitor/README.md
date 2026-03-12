# GCS Monitor Sensor

Monitor a Google Cloud Storage bucket prefix for new objects and trigger jobs automatically.

## Overview

This sensor continuously monitors a GCS bucket prefix for new objects matching a pattern. When objects are detected, it automatically triggers a job and passes object metadata via run_config to downstream assets.

Perfect for:
- Processing files as they arrive in Google Cloud Storage
- Event-driven ETL pipelines starting from GCS ingestion
- Triggering workflows when partners drop files in a shared bucket
- Automated data processing for GCP-based data lakes

## Features

- **Prefix Scoping**: Monitor a specific path within a bucket
- **Pattern Matching**: Use regex to filter specific object names
- **Run Config Support**: Passes object metadata to downstream assets
- **Deduplication**: Tracks processed objects using `updated` timestamps and ETags
- **Directory Placeholder Skipping**: Automatically ignores zero-byte `/`-suffix objects created by some GCS tools
- **Error Handling**: Gracefully handles GCS connectivity and permission errors

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique name for this sensor
- **bucket_name** (string) - Name of the GCS bucket to monitor
- **job_name** (string) - Name of the job to trigger

### Optional Parameters

- **prefix** (string) - Object prefix to scope monitoring (default: `""` monitors the entire bucket)
- **blob_pattern** (string) - Regex matched against the object name relative to the prefix (default: `".*"` matches all objects)
- **minimum_interval_seconds** (integer) - Time between checks in seconds (default: `30`)
- **project** (string) - GCP project ID (default: uses project from ADC)
- **default_status** (string) - Initial sensor status: `running` or `stopped` (default: `"running"`)

## Authentication

This sensor uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials), resolved in this order:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key file
2. Workload Identity (GKE)
3. Attached service account (Compute Engine, Cloud Run, Cloud Functions, etc.)
4. `gcloud auth application-default login` (local development)

No credentials need to be explicitly configured in this component.

### Required IAM Roles

Assign one of the following roles to the identity used by this sensor on the target bucket:

- **Storage Object Viewer** (`roles/storage.objectViewer`) — minimum required
- **Storage Object Admin** (`roles/storage.objectAdmin`) — if your downstream jobs also write to the bucket

## Usage

### Basic Example

Monitor a bucket prefix for Parquet files:

```yaml
# File: defs/components/gcs_sensor.yaml
type: dagster_component_templates.GCSMonitorSensorComponent
attributes:
  sensor_name: gcs_parquet_sensor
  bucket_name: my-data-bucket
  prefix: incoming/
  blob_pattern: ".*\\.parquet$"
  job_name: process_parquet_files
  minimum_interval_seconds: 60
```

### Monitor Multiple File Types

```yaml
type: dagster_component_templates.GCSMonitorSensorComponent
attributes:
  sensor_name: gcs_multi_format_sensor
  bucket_name: analytics-bucket
  prefix: exports/
  blob_pattern: ".*\\.(csv|json|parquet)$"
  job_name: ingest_exported_files
```

### Date-Partitioned Prefix

```yaml
type: dagster_component_templates.GCSMonitorSensorComponent
attributes:
  sensor_name: gcs_daily_sensor
  bucket_name: data-lake
  prefix: raw/events/
  blob_pattern: "\\d{4}-\\d{2}-\\d{2}/.*\\.json\\.gz$"
  job_name: process_daily_events
  minimum_interval_seconds: 300
```

### Monitor Entire Bucket

```yaml
type: dagster_component_templates.GCSMonitorSensorComponent
attributes:
  sensor_name: gcs_bucket_sensor
  bucket_name: partner-drop-zone
  job_name: process_partner_files
  minimum_interval_seconds: 120
```

## Run Config Schema

This sensor passes GCS object information to downstream assets via run_config:

```python
{
  "ops": {
    "config": {
      "bucket": str,        # GCS bucket name
      "name": str,          # Full object name (path within bucket)
      "size": int,          # Object size in bytes
      "etag": str,          # Object ETag
      "updated": str,       # ISO 8601 timestamp of last update
      "content_type": str,  # MIME type (e.g., 'application/octet-stream')
      "prefix": str,        # Prefix used for monitoring
      "project": str        # GCP project ID (empty string if using ADC default)
    }
  }
}
```

## Usage with Assets

### Create Asset That Accepts GCS Object Info

```python
from dagster import asset, Config, AssetExecutionContext

class GCSObjectConfig(Config):
    bucket: str
    name: str
    size: int
    etag: str
    updated: str
    content_type: str
    prefix: str
    project: str

@asset
def process_gcs_object(context: AssetExecutionContext, config: GCSObjectConfig):
    from google.cloud import storage

    client = storage.Client(project=config.project or None)
    bucket = client.bucket(config.bucket)
    blob = bucket.blob(config.name)

    data = blob.download_as_bytes()
    context.log.info(f"Processed gs://{config.bucket}/{config.name} ({config.size} bytes)")
    return data
```

### Read Parquet with pandas

```python
@asset
def load_parquet_from_gcs(context: AssetExecutionContext, config: GCSObjectConfig):
    from google.cloud import storage
    import pandas as pd
    import io

    client = storage.Client(project=config.project or None)
    bucket = client.bucket(config.bucket)
    blob = bucket.blob(config.name)

    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    context.log.info(f"Loaded {len(df)} rows from gs://{config.bucket}/{config.name}")
    return df
```

## Complete Example Pipeline

### 1. GCS Sensor

```yaml
# defs/components/gcs_data_sensor.yaml
type: dagster_component_templates.GCSMonitorSensorComponent
attributes:
  sensor_name: gcs_data_sensor
  bucket_name: my-data-lake
  prefix: raw/csv/
  blob_pattern: ".*\\.csv$"
  job_name: csv_ingestion_job
  minimum_interval_seconds: 60
  project: my-gcp-project
```

### How It Works

1. **Object Arrives**: New file `data_20240115.csv` is uploaded to `gs://my-data-lake/raw/csv/`
2. **Sensor Detects**: Sensor lists objects every 60 seconds, finds objects newer than cursor
3. **Job Triggered**: Sensor creates RunRequest with object metadata
4. **Asset Processes**: Asset downloads and processes the file
5. **Cursor Updated**: Sensor stores the latest `updated` timestamp as the new cursor

## Pattern Examples

### CSV Files Only
```yaml
blob_pattern: ".*\\.csv$"
```

### Gzipped JSON
```yaml
blob_pattern: ".*\\.json\\.gz$"
```

### Parquet in Date Partitions
```yaml
blob_pattern: "\\d{4}/\\d{2}/\\d{2}/.*\\.parquet$"
```

### Files with Specific Prefix in Name
```yaml
blob_pattern: "^export_.*\\.xlsx$"
```

### Avro or ORC Files
```yaml
blob_pattern: ".*\\.(avro|orc)$"
```

## Troubleshooting

### Issue: "Failed to create GCS client"

**Solution**: Verify ADC is configured:
```bash
gcloud auth application-default print-access-token
```

Or set a service account key:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
```

### Issue: "Error listing GCS objects"

**Solutions**:
1. Verify bucket name and project are correct
2. Confirm the identity has `Storage Object Viewer` role on the bucket
3. Check that the prefix exists in the bucket

### Issue: "No new GCS objects found" (when you expect objects)

**Solutions**:
1. Verify the blob pattern matches your object names:
   ```bash
   gsutil ls gs://my-bucket/incoming/ | grep -E "your_pattern"
   ```
2. Check sensor status is "running" in Dagster UI
3. Reset the sensor cursor in the Dagster UI if it has advanced past existing objects

## Performance Considerations

1. **Large Buckets**: Use specific prefixes to reduce the number of objects listed per evaluation
2. **Frequent Uploads**: Increase `minimum_interval_seconds` for high-volume buckets
3. **Avoid Root Prefix**: Always specify a prefix when possible to reduce list overhead

## Requirements

- Python 3.8+
- Dagster 1.5.0+
- google-cloud-storage>=2.0.0
- GCP identity with Storage Object Viewer role on the target bucket

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
