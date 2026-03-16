# Snowflake Resource

Register a dagster-snowflake SnowflakeResource for use by other components

## Installation

`pip install dagster-snowflake`

## Configuration

```yaml
type: dagster_component_templates.SnowflakeResourceComponent
attributes:
  resource_key: snowflake_resource  # key other components use
  # Required
  account: <account>
  user: <user>
  password_env_var: MY_PASSWORD_ENV_VAR  # env var name
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: snowflake_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export PASSWORD_ENV_VAR_VALUE="your-secret-here"
```

