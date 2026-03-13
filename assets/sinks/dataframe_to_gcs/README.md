# DataFrame to GCS

Write a pandas DataFrame to Google Cloud Storage as Parquet, CSV, or JSON using gcsfs.

## Overview

This component receives a DataFrame from an upstream Dagster asset and writes it to a GCS object. It supports Parquet, CSV, and newline-delimited JSON. Authentication is handled via a service account JSON file path or application default credentials.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `bucket_env_var` | `str` | `"GCS_BUCKET"` | Env var containing the GCS bucket name |
| `blob_path` | `str` | required | Path within bucket e.g. `data/output/results.parquet` |
| `format` | `str` | `"parquet"` | Output format: `parquet`, `csv`, or `json` |
| `credentials_env_var` | `Optional[str]` | `None` | Env var with path to service account JSON. None = application default credentials |
| `compression` | `Optional[str]` | `None` | Compression codec. Parquet: `snappy`, `gzip`. CSV: `gzip`. None = default. |
| `project_env_var` | `Optional[str]` | `None` | Env var containing the GCP project ID |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToGcsComponent
attributes:
  asset_name: gcs_session_data
  upstream_asset_key: processed_session_data
  bucket_env_var: GCS_BUCKET
  blob_path: analytics/sessions/output.parquet
  format: parquet
  credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS
  compression: snappy
  project_env_var: GCP_PROJECT_ID
  group_name: storage_sinks
```

## Authentication / Credentials

### Option 1 — Service Account JSON

```bash
export GCS_BUCKET="my-gcs-bucket"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GCP_PROJECT_ID="my-gcp-project"
```

Set `credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS` in your component config.

### Option 2 — Application Default Credentials

Leave `credentials_env_var` unset (or `null`). gcsfs will use:
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable if set
- `gcloud auth application-default login` credentials
- GCE/GKE/Cloud Run metadata server when running on GCP

## format Options

- `parquet` — columnar format, best for analytics. Supports `compression` (`snappy`, `gzip`).
- `csv` — text format, widely compatible. Supports `compression` (`gzip`).
- `json` — newline-delimited JSON (`orient="records"`, `lines=True`).

## Requirements

```
dagster
pandas
gcsfs
pyarrow
```

Install with:

```bash
pip install dagster pandas gcsfs pyarrow
```
