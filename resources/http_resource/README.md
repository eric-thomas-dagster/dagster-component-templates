# Http Resource

Register an HTTP resource (base URL + auth header) for use by other components

## Installation

`pip install requests`

## Configuration

```yaml
type: dagster_component_templates.HTTPResourceComponent
attributes:
  resource_key: http_resource  # key other components use
  # Required
  base_url: <base_url>
  # Optional
  # timeout_seconds: 30
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: http_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export AUTH_HEADER_ENV_VAR_VALUE="your-secret-here"
```

