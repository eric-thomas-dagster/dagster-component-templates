# Gemini Resource

Register a dagster-gemini GeminiResource for use by other components

## Installation

`pip install dagster-gemini`

## Configuration

```yaml
type: dagster_component_templates.GeminiResourceComponent
attributes:
  resource_key: gemini_resource  # key other components use
  # Optional
  # api_key_env_var: GEMINI_API_KEY
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: gemini_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export API_KEY_ENV_VAR_VALUE="your-secret-here"
```

