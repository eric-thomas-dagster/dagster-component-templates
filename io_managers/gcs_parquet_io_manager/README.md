# GCS Parquet IO Manager

Register an IO manager that stores assets as Parquet files on Google Cloud Storage via `gcsfs`.

## Installation

```
pip install gcsfs pandas pyarrow
```

## Configuration

```yaml
type: dagster_component_templates.GCSParquetIOManagerComponent
attributes:
  resource_key: io_manager
  bucket: my-gcs-bucket
  prefix: dagster/assets
  project: my-gcp-project
```

Uses Application Default Credentials (ADC) unless `gcp_credentials_env_var` is set.
