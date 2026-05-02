# GitlabEventSensor

Polls the GitLab Events API for a project. Triggers a RunRequest for each new event since the cursor. Token-authed (project access token or PAT).

## Example

```yaml
type: dagster_component_templates.GitlabEventSensorComponent
attributes:
  sensor_name: gitlab_mr_sensor
  asset_keys: [merge_request_metrics]
  project_id: "12345678"
  token_env_var: GITLAB_TOKEN
  actions: [merged, opened]
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
