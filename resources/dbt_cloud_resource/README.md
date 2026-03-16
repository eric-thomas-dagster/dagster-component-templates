# Dbt Cloud Resource

Register a dagster-dbt DbtCloudClientResource for use by other components

## Installation

`pip install dagster-dbt`

## Configuration

```yaml
type: dagster_component_templates.DbtCloudResourceComponent
attributes:
  resource_key: dbt_cloud_resource  # key other components use
  # Required
  auth_token_env_var: MY_AUTH_TOKEN_ENV_VAR  # env var name
  account_id: <account_id>
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: dbt_cloud_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export AUTH_TOKEN_ENV_VAR_VALUE="your-secret-here"
```

