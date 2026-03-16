# Lance IO Manager

Register a LanceDB IO manager for ML-optimised columnar storage, ideal for embeddings and vector data.

## Installation

```
pip install lancedb pandas
```

## Configuration

```yaml
type: dagster_component_templates.LanceIOManagerComponent
attributes:
  resource_key: io_manager
  base_path: ./lance_data
  table_name_prefix: prod_
```

### S3 / MinIO storage

```yaml
attributes:
  base_path: s3://my-bucket/lance
  s3_access_key_env_var: AWS_ACCESS_KEY_ID
  s3_secret_key_env_var: AWS_SECRET_ACCESS_KEY
  # For MinIO:
  s3_endpoint_url: http://localhost:9000
```

## Use cases

- Storing embedding vectors produced by AI/ML components
- Fast nearest-neighbor search without a separate vector DB
- Columnar ML feature store with versioning
