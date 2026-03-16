# S3 Parquet IO Manager

Register an IO manager that stores assets as Parquet files on Amazon S3 via `s3fs`.

## Installation

```
pip install s3fs pandas pyarrow
```

## Configuration

```yaml
type: dagster_component_templates.S3ParquetIOManagerComponent
attributes:
  resource_key: io_manager
  bucket: my-data-bucket
  prefix: dagster/assets
  region_name: us-east-1
  aws_access_key_env_var: AWS_ACCESS_KEY_ID
  aws_secret_key_env_var: AWS_SECRET_ACCESS_KEY
```

Also works with any S3-compatible store (MinIO, Cloudflare R2) via `endpoint_url`.
