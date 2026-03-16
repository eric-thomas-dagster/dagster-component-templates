# Athena IO Manager

Register an Athena IO manager that stores assets as Parquet on S3 and queries them via Amazon Athena.

## Installation

```
pip install awswrangler boto3
```

## Configuration

```yaml
type: dagster_component_templates.AthenaIOManagerComponent
attributes:
  resource_key: io_manager
  database: dagster_db
  s3_staging_dir: s3://my-bucket/athena-results/
  s3_output_location: s3://my-bucket/assets/
  region_name: us-east-1
```

## Authentication

Uses instance role / environment credentials by default. Set `aws_access_key_env_var` and `aws_secret_key_env_var` for explicit credentials:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
```
