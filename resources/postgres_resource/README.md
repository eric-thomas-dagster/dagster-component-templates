# Postgres Resource

Register a PostgreSQL resource for use by other components

## Installation

`pip install psycopg2-binary`

## Configuration

```yaml
type: dagster_component_templates.PostgresResourceComponent
attributes:
  resource_key: postgres_resource  # key other components use
  # Required
  host: <host>
  database: <database>
  username: <username>
  password_env_var: MY_PASSWORD_ENV_VAR  # env var name
  # Optional
  # port: 5432
  # sslmode: prefer
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: postgres_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export PASSWORD_ENV_VAR_VALUE="your-secret-here"
```

