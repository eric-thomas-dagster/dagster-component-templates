# LakeFSAssetComponent

Run a [lakeFS](https://lakefs.io) **control-plane** operation (commit / merge / create branch) as a Dagster asset.

## What this is — and isn't

This asset performs lakeFS API calls only. It **does not read or write data files**. `deps` declares ordering (so the lakeFS op runs after data assets), not data flow.

For the data plane — actually writing parquet files into a lakeFS-backed bucket — pair this with [`io_managers/lakefs_io_manager`](../../../io_managers/lakefs_io_manager/README.md). The two compose into versioned data pipelines.

## Canonical workflow

```
┌────────────────────────┐
│ create_branch          │  start a per-run branch from main
│ (lakefs_asset)         │
└─────────┬──────────────┘
          │
          ▼
┌────────────────────────┐
│ data assets            │  write parquet to the branch
│ (lakefs_io_manager)    │  (staged, not yet committed)
└─────────┬──────────────┘
          │
          ▼
┌────────────────────────┐
│ commit OR merge        │  seal staged writes into a versioned snapshot,
│ (lakefs_asset)         │  or merge the branch back into main
└────────────────────────┘
```

Without the explicit commit step, lakeFS object writes stay in a "staged" state on the branch and aren't versioned — this asset is what seals them.

## Quick start

```yaml
type: dagster_component_templates.LakeFSAssetComponent
attributes:
  asset_name: data_version_commit
  endpoint: http://localhost:8000
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

Commits all staged (uncommitted) changes on `branch` with the given `commit_message`. Optional `metadata` key-value pairs are attached to the commit object.

```yaml
mode: commit
branch: main
commit_message: "Daily ETL — orders and customers refreshed"
metadata:
  pipeline_run: "2026-04-28-daily"
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

Creates a new branch named `branch` from `source_ref` (a branch name or commit SHA). Use at the start of a pipeline to isolate writes.

```yaml
mode: create_branch
branch: etl-run-2026-04-28
source_ref: main
```

## Authentication

| Env var | Purpose |
|---|---|
| `LAKEFS_ACCESS_KEY_ID` | lakeFS access key (override field name with `access_key_env_var`) |
| `LAKEFS_SECRET_ACCESS_KEY` | lakeFS secret key (override field name with `secret_key_env_var`) |

Credentials are resolved at run-time via `dg.EnvVar` — they don't need to be set when defining the component.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `endpoint` | `str` | required | lakeFS server URL, e.g. `http://localhost:8000` |
| `repository` | `str` | required | lakeFS repository name |
| `access_key_env_var` | `str` | `LAKEFS_ACCESS_KEY_ID` | Env var holding the access key |
| `secret_key_env_var` | `str` | `LAKEFS_SECRET_ACCESS_KEY` | Env var holding the secret key |
| `mode` | `str` | `commit` | One of `commit`, `merge`, `create_branch` |
| `branch` | `str` | `main` | Branch to commit to / source branch |
| `destination_branch` | `str?` | `null` | Target branch for `merge` |
| `source_ref` | `str?` | `null` | Source ref for `create_branch` |
| `commit_message` | `str` | `"Dagster materialization commit"` | Commit message |
| `metadata` | `dict[str, str]?` | `null` | Key-value metadata for the commit |
| `group_name` | `str?` | `data_versioning` | Dagster asset group |
| `deps` | `list[str]?` | `null` | Upstream asset keys for lineage and ordering |

## Notes

- Uses the official [`lakefs`](https://pypi.org/project/lakefs/) Python SDK — same SDK used by `lakefs_io_manager`'s commit-per-materialization mode.
- For atomic per-run versioning, prefer the `create_branch` → write data → `commit` (or `merge`) pattern over committing to `main` directly.
- The `metadata` field accepts only string values per lakeFS's commit metadata schema.
