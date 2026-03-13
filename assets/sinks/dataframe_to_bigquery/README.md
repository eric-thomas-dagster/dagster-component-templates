# DataFrame to BigQuery

Write a pandas DataFrame to a Google BigQuery table using the official BigQuery Python client.

## Overview

This component receives a DataFrame from an upstream Dagster asset and loads it into a BigQuery table. It supports all three BigQuery write dispositions and both service account and application default credential authentication.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `table_id` | `str` | required | Full BigQuery table ID: `project.dataset.table` |
| `write_disposition` | `str` | `"WRITE_TRUNCATE"` | `WRITE_TRUNCATE`, `WRITE_APPEND`, or `WRITE_EMPTY` |
| `credentials_env_var` | `Optional[str]` | `None` | Env var with path to service account JSON. None = application default credentials |
| `location` | `Optional[str]` | `None` | BigQuery location e.g. `US`, `EU`, `us-central1` |
| `chunksize` | `Optional[int]` | `None` | Number of rows per API call (None = client default) |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToBigqueryComponent
attributes:
  asset_name: bigquery_events
  upstream_asset_key: processed_events
  table_id: my-gcp-project.analytics.events
  write_disposition: WRITE_TRUNCATE
  credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS
  location: US
  chunksize: 50000
  group_name: warehouse_sinks
```

## Authentication / Credentials

### Option 1 — Service Account JSON (recommended for production)

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

Set `credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS` in your component config.

### Option 2 — Application Default Credentials

Leave `credentials_env_var` unset (or `null`). The client will automatically use:
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable if set
- `gcloud auth application-default login` credentials
- GCE/GKE/Cloud Run metadata server when running on GCP

## write_disposition Options

- `WRITE_TRUNCATE` — deletes all existing rows before inserting new data.
- `WRITE_APPEND` — appends new rows to the existing table.
- `WRITE_EMPTY` — fails if the table already contains rows.

## Requirements

```
dagster
pandas
google-cloud-bigquery
db-dtypes
```

Install with:

```bash
pip install dagster pandas google-cloud-bigquery db-dtypes
```
