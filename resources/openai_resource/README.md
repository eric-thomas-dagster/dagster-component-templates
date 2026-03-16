# Openai Resource

Register a dagster-openai OpenAIResource for use by other components

## Installation

`pip install dagster-openai`

## Configuration

```yaml
type: dagster_component_templates.OpenAIResourceComponent
attributes:
  resource_key: openai_resource  # key other components use
  # Optional
  # api_key_env_var: OPENAI_API_KEY
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: openai_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export API_KEY_ENV_VAR_VALUE="your-secret-here"
```

