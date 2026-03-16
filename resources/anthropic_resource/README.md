# Anthropic Resource

Register a dagster-anthropic AnthropicResource for use by other components

## Installation

`pip install dagster-anthropic`

## Configuration

```yaml
type: dagster_component_templates.AnthropicResourceComponent
attributes:
  resource_key: anthropic_resource  # key other components use
  # Optional
  # api_key_env_var: ANTHROPIC_API_KEY
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: anthropic_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export API_KEY_ENV_VAR_VALUE="your-secret-here"
```

