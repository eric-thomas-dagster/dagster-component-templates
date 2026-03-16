# Databricks IO Manager

Register an IO manager that reads and writes Pandas DataFrames as Unity Catalog Delta tables via the Databricks SQL Connector.

## Installation

```
pip install databricks-sql-connector pandas
```

## Configuration

```yaml
type: dagster_component_templates.DatabricksIOManagerComponent
attributes:
  resource_key: io_manager
  server_hostname: adb-1234567890.12.azuredatabricks.net
  http_path: /sql/1.0/warehouses/abc123def456
  access_token_env_var: DATABRICKS_TOKEN
  catalog: main
  schema_name: dagster_assets
```

## Authentication

```bash
export DATABRICKS_TOKEN=dapi...
```

Generate a personal access token in Databricks: User Settings → Developer → Access Tokens.
