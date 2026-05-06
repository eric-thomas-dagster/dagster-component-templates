# DataFrame to Databricks

Write a pandas DataFrame to a Databricks Delta Lake table using the Databricks SQL Connector.

## Overview

This component receives a DataFrame from an upstream Dagster asset and writes it to a Databricks Unity Catalog table. It connects via the Databricks SQL Connector and inserts rows in batches. When `mode` is `overwrite`, the table is recreated using a `CREATE OR REPLACE TABLE` statement before insertion.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `catalog` | `str` | `"main"` | Unity Catalog name |
| `schema` | `str` | required | Target schema/database name |
| `table` | `str` | required | Target table name |
| `host_env_var` | `str` | `"DATABRICKS_HOST"` | Env var containing Databricks workspace URL |
| `token_env_var` | `str` | `"DATABRICKS_TOKEN"` | Env var containing Databricks personal access token |
| `mode` | `str` | `"overwrite"` | Write mode: `overwrite`, `append`, `ignore`, `error` |
| `merge_schema` | `bool` | `True` | Allow schema evolution when appending |
| `cluster_id` | `Optional[str]` | `None` | SQL warehouse or cluster ID (None = auto) |
| `drop_timezone` | `bool` | `True` | Drop tz from tz-aware datetime columns before INSERT — the parameterized INSERT path doesn't bind tz cleanly. Set `false` if you handle tz upstream. |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToDatabricksComponent
attributes:
  asset_name: databricks_transactions
  upstream_asset_key: processed_transactions
  catalog: main
  schema: analytics
  table: transactions
  host_env_var: DATABRICKS_HOST
  token_env_var: DATABRICKS_TOKEN
  mode: overwrite
  merge_schema: true
  cluster_id: abc1234567890def
  group_name: warehouse_sinks
```

## Authentication / Credentials

```bash
export DATABRICKS_HOST="https://adb-1234567890123456.7.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
```

The `cluster_id` should be the ID of your SQL Warehouse (found in the warehouse settings URL). If not set, the connector will attempt to use `auto` routing.

## mode Options

- `overwrite` — drops and recreates the table, then inserts all rows.
- `append` — inserts rows into the existing table without removing prior data.
- `ignore` — skips the write if the table already has rows.
- `error` — raises a `RuntimeError` if the table already exists.

## Requirements

```
dagster
pandas
databricks-sql-connector
```

Install with:

```bash
pip install dagster pandas databricks-sql-connector
```


## Azure Databricks

Azure Databricks is the same Databricks platform, deployed in Azure with
host URL `https://adb-XXX.azuredatabricks.net`. This component works
out-of-the-box with Azure Databricks via the standard `host` /
`server_hostname` / `host_env_var` field — no Azure-specific code needed.

**Auth options for Azure Databricks:**
- Personal Access Token (PAT) — same as multi-cloud, simplest
- Microsoft Entra OAuth (preferred for production) — set
  `ARM_TENANT_ID`, `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET` env vars; the
  Databricks SDK auto-detects and uses OAuth
- Managed identity (Azure compute only) — set `DATABRICKS_AZURE_RESOURCE_ID`
  to the workspace ARM resource ID; SDK uses the attached MSI

The service principal needs the **Contributor** role on the workspace,
plus appropriate Unity Catalog / cluster permissions inside Databricks.

