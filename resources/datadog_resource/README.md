# Datadog Resource

Register a dagster-datadog DatadogResource for use by other components

## Installation

`pip install dagster-datadog`

## Configuration

```yaml
type: dagster_component_templates.DatadogResourceComponent
attributes:
  resource_key: datadog_resource  # key other components use
  # Required
  api_key_env_var: MY_API_KEY_ENV_VAR  # env var name
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: datadog_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export API_KEY_ENV_VAR_VALUE="your-secret-here"
```

