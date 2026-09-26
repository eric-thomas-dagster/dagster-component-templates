# `AsanaTaskCreateComponent`

Reverse-ETL sink: **create one Asana task per row** of an upstream DataFrame.

> **Not an upsert.** Asana has no upsert concept for tasks — every materialization creates one new task per row. Re-running this on the same data creates duplicate tasks. This is for "spin up a task per flagged row" patterns (e.g. one task per data-quality failure, one task per at-risk account), not for mirroring a table that should stay in sync over time.

## When to use

- Auto-create Asana tasks from anomaly detection, data-quality check failures, or any other "this row needs a human to look at it" workflow.

## Prerequisites

1. **At least one project GID** (`project_gids`) unless the `asana_resource` is configured with a default `workspace_gid` — a task needs somewhere to live.
2. **Custom fields already created** in the target project for anything in `custom_fields_map`.

## Pairs with

- **`asana_resource`** — personal access token auth (required).
- **`asana_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `asana_resource`) | Resource key registered by AsanaResourceComponent. |
| `name_column` | required | Upstream column holding the task name. |
| `notes_column` | optional | Upstream column holding the task notes/description. |
| `project_gids` | optional | Asana project GIDs to add every created task to. |
| `assignee_column` | optional | Upstream column holding an Asana user GID or email. |
| `custom_fields_map` | optional | Upstream column -> Asana custom_field GID. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.AsanaTaskCreateComponent
attributes:
  asset_name: asana_dq_failure_tasks
  upstream_asset_key: dq_check_failures
  resource_key: asana_resource
  name_column: failure_summary
  notes_column: failure_detail
  project_gids: ["1201234567890"]
  group_name: reverse_etl
```
