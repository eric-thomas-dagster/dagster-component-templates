# Personio Resource

Register a `PersonioResource` wrapping the `personio-py` client for use by other Dagster components.

## Installation

```bash
pip install personio-py
```

## Configuration

```yaml
type: dagster_component_templates.PersonioResourceComponent
attributes:
  resource_key: personio_resource  # key other components use
  client_id_env_var: PERSONIO_CLIENT_ID  # env var holding your client ID
  client_secret_env_var: PERSONIO_CLIENT_SECRET  # env var holding your client secret
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: personio_resource
```

## Authentication

Both secret env var fields accept **environment variable names** (not the secret values themselves).
Set the corresponding environment variables before running Dagster:

```bash
export PERSONIO_CLIENT_ID="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
export PERSONIO_CLIENT_SECRET="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

The resource calls `client.authenticate()` automatically when `get_client()` is invoked. Create API credentials in Personio under Settings > Integrations > API Credentials.
