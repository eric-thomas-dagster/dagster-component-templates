# Twilio Resource

Register a dagster-twilio TwilioResource for use by other components

## Installation

`pip install dagster-twilio`

## Configuration

```yaml
type: dagster_component_templates.TwilioResourceComponent
attributes:
  resource_key: twilio_resource  # key other components use
  # Required
  account_sid_env_var: <account_sid_env_var>
  auth_token_env_var: MY_AUTH_TOKEN_ENV_VAR  # env var name
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: twilio_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export AUTH_TOKEN_ENV_VAR_VALUE="your-secret-here"
```

