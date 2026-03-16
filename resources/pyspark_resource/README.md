# Pyspark Resource

Register a dagster-pyspark PySparkResource for use by other components

## Installation

`pip install dagster-pyspark`

## Configuration

```yaml
type: dagster_component_templates.PySparkResourceComponent
attributes:
  resource_key: pyspark_resource  # key other components use
  # Optional
  # app_name: dagster
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: pyspark_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

