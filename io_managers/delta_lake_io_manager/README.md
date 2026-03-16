# Delta Lake IO Manager

Register a `DeltaLakePandasIOManager` so assets are stored as Delta tables — wraps the official `dagster-deltalake-pandas` integration. Supports local disk, S3, GCS, and Azure ADLS storage.

## Installation

```
pip install dagster-deltalake-pandas
```

## Configuration

### Local storage
```yaml
attributes:
  root_uri: /data/delta
```

### S3
```yaml
attributes:
  root_uri: s3://my-bucket/delta
  aws_access_key_env_var: AWS_ACCESS_KEY_ID
  aws_secret_key_env_var: AWS_SECRET_ACCESS_KEY
  aws_region: us-east-1
```

### Azure ADLS
```yaml
attributes:
  root_uri: az://my-container/delta
  azure_storage_account_env_var: AZURE_STORAGE_ACCOUNT
  azure_storage_key_env_var: AZURE_STORAGE_KEY
```
