# GithubEventSensor

Polls the GitHub Events API for a repo and triggers a RunRequest for each new event of the specified types. Cursor stored as last-seen event ID. Personal access token used via env var.

## Example

```yaml
type: dagster_component_templates.GithubEventSensorComponent
attributes:
  sensor_name: dagster_pr_sensor
  asset_keys: [open_pr_count]
  repo: "dagster-io/dagster"
  token_env_var: GITHUB_TOKEN
  event_types: [PullRequestEvent]
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
