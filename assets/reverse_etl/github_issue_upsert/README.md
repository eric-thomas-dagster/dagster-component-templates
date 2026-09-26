# `GitHubIssueUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **GitHub Issues** in a target repo. Rows are matched to existing issues by a stable key marker embedded in the issue body (`<!-- dagster-key: <value> -->`), not by title — the title can be edited by humans without breaking the sync.

Matches are updated (title, body, labels, state, assignees). Misses are inserted as new issues. Optional `close_missing: true` closes issues whose key marker is not in the upstream DataFrame.

## When to use

- Auto-create/update GitHub issues from a data-quality pipeline, an incident tracker mart, or any other "this needs an engineer's attention" workflow.

## Pairs with

- **`github_resource`** — connection (required).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `repo` | required | Target repo in 'owner/name' form. |
| `resource_key` | optional (default `github`) | Resource key registered by GithubResourceComponent. |
| `key_column` | required | Upstream column holding a stable unique key. |
| `title_column` | required | Column holding the issue title. |
| `body_column` | optional | Column holding the issue body (markdown). |
| `labels_column` | optional | Column holding labels (list or comma-separated string). |
| `state_column` | optional | Column holding state ('open'/'closed'). |
| `assignees_column` | optional | Column holding assignee logins. |
| `default_labels` | optional | Labels always applied on top of labels_column. |
| `close_missing` | optional (default `false`) | Close open issues whose key marker is not in the upstream DataFrame. |
| `batch_size` | optional (default `100`) | Max upstream rows to process per run. |

## Example
```yaml
type: dagster_component_templates.GitHubIssueUpsertComponent
attributes:
  asset_name: github_incidents_mirror
  upstream_asset_key: incidents_seed
  repo: my-org/incidents-tracker
  resource_key: github
  key_column: incident_id
  title_column: name
  body_column: description
  labels_column: labels
  state_column: state
  default_labels: [auto-synced]
```
