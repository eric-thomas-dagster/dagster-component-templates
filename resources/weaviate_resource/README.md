# Weaviate Resource

Register a dagster-weaviate WeaviateResource for use by other components

## Installation

`pip install dagster-weaviate`

## Configuration

```yaml
type: dagster_component_templates.WeaviateResourceComponent
attributes:
  resource_key: weaviate_resource  # key other components use
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: weaviate_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export API_KEY_ENV_VAR_VALUE="your-secret-here"
```

