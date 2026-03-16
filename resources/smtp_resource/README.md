# Smtp Resource

Register an SMTP resource for sending emails from other components

## Installation

No extra packages required (uses Python stdlib)

## Configuration

```yaml
type: dagster_component_templates.SMTPResourceComponent
attributes:
  resource_key: smtp_resource  # key other components use
  # Required
  host: <host>
  username: <username>
  password_env_var: MY_PASSWORD_ENV_VAR  # env var name
  # Optional
  # port: 587
  # use_tls: True
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: smtp_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export PASSWORD_ENV_VAR_VALUE="your-secret-here"
```

