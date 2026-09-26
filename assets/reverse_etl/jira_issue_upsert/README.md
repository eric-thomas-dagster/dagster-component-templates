# `JiraIssueUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Jira issues** in a target project. Rows are matched to existing issues by a Jira label of the form `dagsterkey-<value>`, where `<value>` comes from `key_column`. JQL filters by label server-side, so this scales cleanly.

Matches are updated (summary, description, labels, status transition). Misses are created.

## When to use

- Auto-create/update Jira tickets from a data-quality pipeline, an incident tracker mart, or any other "this needs an engineer's attention" workflow.

## Pairs with

- **`jira_resource`** — connection (required).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `project_key` | required | Target Jira project key (e.g. 'SCRATCH'). |
| `resource_key` | optional (default `jira`) | Resource key registered by JiraResourceComponent. |
| `key_column` | required | Upstream column holding a stable unique key. |
| `summary_column` | required | Column holding the issue summary (title). |
| `description_column` | optional | Column holding the issue description. |
| `labels_column` | optional | Column holding labels. |
| `transition_column` | optional | Column holding a workflow transition name to apply after upsert. |
| `priority_column` | optional | Column holding priority name. |
| `issue_type` | optional (default `Task`) | Issue type to use for new issues. |
| `default_labels` | optional | Labels always applied on top of labels_column. |
| `batch_size` | optional (default `100`) | Max upstream rows to process per run. |

## Example
```yaml
type: dagster_component_templates.JiraIssueUpsertComponent
attributes:
  asset_name: jira_incidents_mirror
  upstream_asset_key: incidents_seed
  project_key: SCRATCH
  resource_key: jira
  key_column: incident_id
  summary_column: name
  description_column: description
  labels_column: labels
  transition_column: status
  issue_type: Task
  default_labels: [auto-synced]
```
