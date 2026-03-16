# Aws S3 Resource

Register a dagster-aws S3Resource for use by other components

## Installation

`pip install dagster-aws`

## Configuration

```yaml
type: dagster_component_templates.S3ResourceComponent
attributes:
  resource_key: s3_resource  # key other components use
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: s3_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export AWS_SECRET_ACCESS_KEY_ENV_VAR_VALUE="your-secret-here"
```

