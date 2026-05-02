# LinearIssueSensor

Polls Linear's GraphQL API for issues updated since the cursor. Triggers a RunRequest per matching issue. Token-authed.

## Example

```yaml
type: dagster_component_templates.LinearIssueSensorComponent
attributes:
  sensor_name: linear_active_issues_sensor
  asset_keys: [linear_team_metrics]
  api_key_env_var: LINEAR_API_KEY
  state_types: [started, unstarted]
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
