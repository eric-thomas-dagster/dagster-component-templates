# MSTeamsResource

Wraps `dagster-msteams`'s `MSTeamsResource` so any asset / op can post messages to a Teams channel via an incoming webhook. Pair with hooks (failure_hook, etc.) for run-status notifications.

## Example

```yaml
type: dagster_component_templates.MSTeamsResourceComponent
attributes:
  webhook_url_env_var: <fill in>
  timeout_seconds: <fill in>
  resource_key: <fill in>
```

## Requirements

```
dagster
dagster-msteams
```
