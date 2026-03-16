# Pagerduty Resource

Register a dagster-pagerduty PagerDutyService for use by other components

## Installation

`pip install dagster-pagerduty`

## Configuration

```yaml
type: dagster_component_templates.PagerDutyResourceComponent
attributes:
  resource_key: pagerduty_resource  # key other components use
  # Required
  routing_key_env_var: MY_ROUTING_KEY_ENV_VAR  # env var name
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: pagerduty_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export ROUTING_KEY_ENV_VAR_VALUE="your-secret-here"
```

