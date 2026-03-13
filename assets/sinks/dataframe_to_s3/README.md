# DataFrame to S3

Write a pandas DataFrame to Amazon S3 as Parquet, CSV, or JSON using s3fs.

## Overview

This component receives a DataFrame from an upstream Dagster asset and writes it to an S3 object. It supports Parquet (with optional partitioning), CSV, and newline-delimited JSON. Authentication is handled via explicit AWS key env vars or the standard AWS credential chain (IAM roles, instance profiles, `~/.aws/credentials`).

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `bucket_env_var` | `str` | `"S3_BUCKET"` | Env var containing the S3 bucket name |
| `key` | `str` | required | S3 object key / path within bucket e.g. `data/output/results.parquet` |
| `format` | `str` | `"parquet"` | Output format: `parquet`, `csv`, or `json` |
| `aws_access_key_env_var` | `Optional[str]` | `None` | Env var with AWS access key ID. None = use IAM role or `~/.aws/credentials` |
| `aws_secret_key_env_var` | `Optional[str]` | `None` | Env var with AWS secret access key |
| `aws_region` | `Optional[str]` | `None` | AWS region e.g. `us-east-1` |
| `compression` | `Optional[str]` | `None` | Compression codec. Parquet: `snappy`, `gzip`. CSV: `gzip`. None = default. |
| `partition_cols` | `Optional[List[str]]` | `None` | Column names to partition by (parquet only) |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToS3Component
attributes:
  asset_name: s3_daily_report
  upstream_asset_key: processed_daily_report
  bucket_env_var: S3_BUCKET
  key: data/reports/daily_report.parquet
  format: parquet
  aws_access_key_env_var: AWS_ACCESS_KEY_ID
  aws_secret_key_env_var: AWS_SECRET_ACCESS_KEY
  aws_region: us-east-1
  compression: snappy
  partition_cols:
    - date
  group_name: storage_sinks
```

## Authentication / Credentials

### Option 1 — Explicit credentials

```bash
export S3_BUCKET="my-data-bucket"
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Option 2 — IAM role / instance profile / AWS profile

Leave `aws_access_key_env_var` and `aws_secret_key_env_var` unset. The standard AWS credential chain will be used automatically (environment variables, `~/.aws/credentials`, EC2 instance metadata, ECS task role, etc.).

## format Options

- `parquet` — columnar format, best for analytics. Supports `partition_cols` and `compression` (`snappy`, `gzip`).
- `csv` — text format, widely compatible. Supports `compression` (`gzip`).
- `json` — newline-delimited JSON (`orient="records"`, `lines=True`).

## Requirements

```
dagster
pandas
boto3
s3fs
pyarrow
```

Install with:

```bash
pip install dagster pandas boto3 s3fs pyarrow
```
