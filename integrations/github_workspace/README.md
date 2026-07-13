# GitHubWorkspaceComponent

Auto-emit one Dagster asset per **GitHub Actions workflow** across every repo in an org (or user). `StateBackedComponent` — discovery cached to disk. Materializing an asset reads the most recent workflow runs and emits a DataFrame.

The workspace-shape peer of `github_ingestion` / `github_event_sensor`.

## Example

```yaml
type: dagster_community_components.GitHubWorkspaceComponent
attributes:
  owner_env_var: GITHUB_OWNER
  token_env_var: GITHUB_TOKEN
  repo_selector:
    by_pattern: ["dagster-*", "*-pipelines"]
  workflow_selector:
    by_pattern: ["CI*", "Deploy*"]
  recent_runs_limit: 20
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Two-level filtering

- `repo_selector` — narrow which repos are enumerated
- `workflow_selector` — narrow which workflows within those repos become assets

Both are Fivetran-shape (by_name / by_pattern / exclude_*).

## Emitted DataFrame schema

Each asset materialization returns:
- `id`, `run_number`, `status`, `conclusion`, `event`, `head_branch`, `actor`, `created_at`, `updated_at`

## Related

- `github_resource`
- `github_ingestion` — REST endpoint ingestion (issues/PRs/commits)
- `github_event_sensor` — event-driven trigger on repo events
- `github_audit_log_ingestion` — org audit log
