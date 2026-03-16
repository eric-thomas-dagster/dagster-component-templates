# MinIO IO Manager

Register a Delta Lake IO manager pointed at MinIO for S3-compatible lakehouse storage.

## Installation

```
pip install deltalake pandas
```

## Configuration

```yaml
type: dagster_component_templates.MinIOIOManagerComponent
attributes:
  resource_key: io_manager
  endpoint_url: http://localhost:9000
  access_key_env_var: MINIO_ACCESS_KEY
  secret_key_env_var: MINIO_SECRET_KEY
  bucket: lake
  prefix: assets
```

## Authentication

```bash
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
```
