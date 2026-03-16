# Databricks Resource

Register a Databricks workspace resource for use by other components

## Installation

`pip install dagster-databricks`

## Configuration

```yaml
type: dagster_component_templates.DatabricksResourceComponent
attributes:
  resource_key: databricks_resource  # key other components use
  # Required
  host: <host>
  token_env_var: MY_TOKEN_ENV_VAR  # env var name
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: databricks_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export TOKEN_ENV_VAR_VALUE="your-secret-here"
```

