# JiraIssueSensor

Polls Jira's search API with a JQL query (auto-augmented with `updated >= cursor`). Triggers a RunRequest per new/updated issue. Basic-authed via API token.

## Example

```yaml
type: dagster_component_templates.JiraIssueSensorComponent
attributes:
  sensor_name: ops_critical_jira_sensor
  asset_keys: [incident_report]
  base_url: "https://mycompany.atlassian.net"
  email: ops@mycompany.com
  api_token_env_var: JIRA_API_TOKEN
  jql: "project = OPS AND priority = Highest"
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
