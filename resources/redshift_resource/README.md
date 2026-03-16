# Redshift Resource

Register a dagster-aws RedshiftClientResource for use by other components

## Installation

`pip install dagster-aws`

## Configuration

```yaml
type: dagster_component_templates.RedshiftResourceComponent
attributes:
  resource_key: redshift_resource  # key other components use
  # Required
  host: <host>
  database: <database>
  user: <user>
  password_env_var: MY_PASSWORD_ENV_VAR  # env var name
  # Optional
  # port: 5439
  # autocommit: False
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: redshift_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export PASSWORD_ENV_VAR_VALUE="your-secret-here"
```

