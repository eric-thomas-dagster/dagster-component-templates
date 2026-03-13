# LakeFSAssetComponent

Interact with a [lakeFS](https://lakefs.io/) repository for data versioning as a Dagster asset.

## Use case

lakeFS brings Git-like branching and versioning to data lakes. `LakeFSAssetComponent` lets you embed data versioning operations directly into your Dagster pipeline:

- **Commit** — after upstream assets write data, commit the staged changes to a branch so the state is versioned and reproducible.
- **Merge** — promote changes from a development or staging branch into main after validation assets pass.
- **Create branch** — snapshot a source ref as a new branch at the start of a pipeline run for isolation.

All operations use the lakeFS REST API with HTTP Basic authentication via `requests`. No lakeFS SDK dependency is required.

## Prerequisites

Install `requests` (already present in most Python environments):

```bash
pip install requests
```

Set the required environment variables:

```bash
export LAKEFS_ENDPOINT=http://localhost:8000
export LAKEFS_ACCESS_KEY_ID=your-access-key
export LAKEFS_SECRET_ACCESS_KEY=your-secret-key
```

## Quick start

```yaml
type: dagster_component_templates.LakeFSAssetComponent
attributes:
  asset_name: data_version_commit
  repository: my-data-lake
  mode: commit
  branch: main
  commit_message: "Daily ETL materialization"
  group_name: data_versioning
  deps:
    - marts/orders
    - marts/customers
```

## Operation modes

### commit

Commits all staged (uncommitted) changes on `branch` with the given `commit_message`. Optional `metadata` key-value pairs are attached to the commit.

```yaml
mode: commit
branch: main
commit_message: "Daily ETL - orders and customers refreshed"
metadata:
  pipeline_run: "2024-01-15-daily"
  source: "dagster"
```

### merge

Merges `branch` (source) into `destination_branch`. Useful for promoting validated data from a staging branch to main.

```yaml
mode: merge
branch: staging
destination_branch: main
```

### create_branch

Creates a new branch named `branch` from `source_ref` (a branch name or commit SHA). Use this at the start of a pipeline run to isolate writes.

```yaml
mode: create_branch
branch: etl-run-2024-01-15
source_ref: main
```

## Commit metadata

The `metadata` field (commit mode only) accepts string key-value pairs and is stored as part of the lakeFS commit object:

```yaml
metadata:
  dagster_run_id: "abc123"
  data_date: "2024-01-15"
  team: "data-engineering"
```

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `repository` | `str` | required | lakeFS repository name |
| `lakefs_endpoint_env_var` | `str` | `"LAKEFS_ENDPOINT"` | Env var with lakeFS URL |
| `access_key_env_var` | `str` | `"LAKEFS_ACCESS_KEY_ID"` | Env var with access key |
| `secret_key_env_var` | `str` | `"LAKEFS_SECRET_ACCESS_KEY"` | Env var with secret key |
| `mode` | `str` | `"commit"` | Operation: `commit`, `merge`, `create_branch` |
| `branch` | `str` | `"main"` | Branch to commit to / source branch |
| `destination_branch` | `str` | `None` | Target branch for merge mode |
| `source_ref` | `str` | `None` | Source ref for create_branch mode |
| `commit_message` | `str` | `"Dagster materialization commit"` | Commit message |
| `metadata` | `dict[str, str]` | `None` | Key-value metadata for the commit |
| `group_name` | `str` | `"data_versioning"` | Dagster asset group |
| `deps` | `list[str]` | `None` | Upstream asset keys |
