# JiraWorkspaceComponent

Auto-emit one Dagster asset per **Jira project**. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs a JQL query in that project and emits issues as a DataFrame.

The workspace-shape peer of `jira_ingestion`.

## Example

```yaml
type: dagster_community_components.JiraWorkspaceComponent
attributes:
  base_url_env_var: JIRA_URL
  email_env_var: JIRA_EMAIL
  api_token_env_var: JIRA_API_TOKEN
  project_selector:
    by_name: [ENG, OPS, PROD]
  jql_template: "project = {project_key} AND updated >= -7d ORDER BY updated DESC"
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## JQL template

Every asset runs `jql_template.format(project_key=<key>)`. Default is `"project = {project_key} ORDER BY created DESC"`. Common patterns:

- `"project = {project_key} AND updated >= -7d"` — rolling last-week window
- `"project = {project_key} AND status != Done"` — open work only
- `"project = {project_key} AND labels = data-eng"` — team-scoped filter

## Related

- `jira_resource`
- `jira_ingestion` — single-JQL DataFrame ingestion (this workspace's per-project shape)
- `jira_issue_sensor` — event-driven trigger on issue state
