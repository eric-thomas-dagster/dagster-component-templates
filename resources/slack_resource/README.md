# Slack Resource

Register a dagster-slack SlackResource for use by other components

## Installation

`pip install dagster-slack`

## Configuration

```yaml
type: dagster_component_templates.SlackResourceComponent
attributes:
  resource_key: slack_resource  # key other components use
  # Required
  token_env_var: MY_TOKEN_ENV_VAR  # env var name
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: slack_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export TOKEN_ENV_VAR_VALUE="your-secret-here"
```

