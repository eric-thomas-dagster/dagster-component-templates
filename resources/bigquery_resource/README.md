# Bigquery Resource

Register a dagster-gcp BigQueryResource for use by other components

## Installation

`pip install dagster-gcp`

## Configuration

```yaml
type: dagster_component_templates.BigQueryResourceComponent
attributes:
  resource_key: bigquery_resource  # key other components use
  # Required
  project: <project>
  # Optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: bigquery_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export GCP_CREDENTIALS_ENV_VAR_VALUE="your-secret-here"
```

