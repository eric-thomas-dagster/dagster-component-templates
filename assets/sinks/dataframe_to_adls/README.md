# DataFrame to ADLS

Write a pandas DataFrame to Azure Data Lake Storage Gen2 as Parquet, CSV, or JSON using adlfs.

## Overview

This component receives a DataFrame from an upstream Dagster asset and writes it to an ADLS Gen2 container. It supports account key, connection string, and Azure AD / managed identity authentication. Files are written using the `abfs://` URI scheme with pandas filesystem abstraction.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `account_name_env_var` | `str` | `"AZURE_STORAGE_ACCOUNT"` | Env var containing the Azure storage account name |
| `container` | `str` | required | ADLS container / filesystem name |
| `blob_path` | `str` | required | Path within container e.g. `processed/results.parquet` |
| `format` | `str` | `"parquet"` | Output format: `parquet`, `csv`, or `json` |
| `account_key_env_var` | `Optional[str]` | `None` | Env var with storage account key (None = Azure AD / managed identity) |
| `connection_string_env_var` | `Optional[str]` | `None` | Env var with full Azure connection string (alternative to `account_key_env_var`) |
| `compression` | `Optional[str]` | `None` | Compression codec. Parquet: `snappy`, `gzip`. CSV: `gzip`. None = default. |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToAdlsComponent
attributes:
  asset_name: adls_user_events
  upstream_asset_key: processed_user_events
  account_name_env_var: AZURE_STORAGE_ACCOUNT
  container: datalake
  blob_path: processed/user_events/results.parquet
  format: parquet
  account_key_env_var: AZURE_STORAGE_KEY
  compression: snappy
  group_name: storage_sinks
```

## Authentication / Credentials

### Option 1 — Storage Account Key

```bash
export AZURE_STORAGE_ACCOUNT="mystorageaccount"
export AZURE_STORAGE_KEY="base64encodedkeyhere=="
```

### Option 2 — Connection String

```bash
export AZURE_STORAGE_ACCOUNT="mystorageaccount"
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

Set `connection_string_env_var: AZURE_STORAGE_CONNECTION_STRING` in your component config.

### Option 3 — Azure AD / Managed Identity

Leave both `account_key_env_var` and `connection_string_env_var` unset. adlfs will use the `DefaultAzureCredential` chain (managed identity, environment credentials, Azure CLI, etc.).

## format Options

- `parquet` — columnar format, best for analytics. Supports `compression` (`snappy`, `gzip`).
- `csv` — text format, widely compatible. Supports `compression` (`gzip`).
- `json` — newline-delimited JSON (`orient="records"`, `lines=True`).

## Requirements

```
dagster
pandas
adlfs
pyarrow
```

Install with:

```bash
pip install dagster pandas adlfs pyarrow
```
