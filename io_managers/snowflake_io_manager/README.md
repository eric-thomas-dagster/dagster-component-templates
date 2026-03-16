# Snowflake IO Manager

Register a `SnowflakePandasIOManager` so assets are automatically stored in and loaded from Snowflake — wraps the official `dagster-snowflake-pandas` integration.

## Installation

```
pip install dagster-snowflake-pandas
```

## Configuration

```yaml
type: dagster_component_templates.SnowflakeIOManagerComponent
attributes:
  resource_key: io_manager
  account: xy12345.us-east-1
  user: dagster_user
  password_env_var: SNOWFLAKE_PASSWORD
  database: ANALYTICS
  warehouse: COMPUTE_WH
  schema_name: PUBLIC
```

## Authentication

```bash
export SNOWFLAKE_PASSWORD=your-password
```
