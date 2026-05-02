# PagerDutyIncidentSensor

Polls PagerDuty's REST API for incidents. Triggers a RunRequest per new incident since the cursor (incident `created_at`). Token-authed.

## Example

```yaml
type: dagster_component_templates.PagerDutyIncidentSensorComponent
attributes:
  sensor_name: high_severity_incident_sensor
  asset_keys: [incident_post_mortem]
  api_key_env_var: PAGERDUTY_API_KEY
  statuses: [triggered]
  urgencies: [high]
  minimum_interval_seconds: 60
```

## Requirements

```
requests
```
