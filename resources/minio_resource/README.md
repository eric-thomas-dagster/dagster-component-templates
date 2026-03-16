# MinIO Resource

Register a MinIO S3-compatible object storage resource for use by other components.

## Installation

```
pip install minio
```

## Configuration

```yaml
type: dagster_component_templates.MinIOResourceComponent
attributes:
  resource_key: minio_resource
  endpoint: localhost:9000
  access_key_env_var: MINIO_ACCESS_KEY
  secret_key_env_var: MINIO_SECRET_KEY
  secure: false
  default_bucket: lake
```

## Authentication

Set the corresponding environment variables before running Dagster:

```bash
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
```

## Usage in other components

```yaml
attributes:
  resource_key: minio_resource
```
