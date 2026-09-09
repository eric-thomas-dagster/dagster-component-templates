# DagsterCheckResultsPruneJobComponent

Op-shaped job that deletes old `AssetCheckResult` events from the event
log. Similar spirit to `dagster_asset_materialization_prune_job` but
targets asset check evaluations.

## Behavior

1. For each `asset_keys[]` entry (or all assets when empty):
   - Queries `ASSET_CHECK_EVALUATION` events older than `max_age_days`.
   - Calls `event_log_storage.delete_events([storage_id, ...])` per record.
2. Hard-capped by `max_deletes_per_run` per tick.

**Backend caveat:** the `delete_events` API isn't uniformly implemented
across all event log backends. Sqlite / Postgres event log storage
supports it; some custom or older backends may not. If your backend
doesn't expose `delete_events`, this job logs a warning and no-ops.

## YAML example

```yaml
type: dagster_component_templates.DagsterCheckResultsPruneJobComponent
attributes:
  job_name: prune_asset_check_results
  schedule: "0 5 * * 0"
  default_status: STOPPED
  asset_keys: []           # [] = all assets
  max_age_days: 30
  max_deletes_per_run: 5000
  dry_run: true            # verify first
```

## Options

- `asset_keys` — optional scope. Empty = all.
- `max_age_days` — retention window.
- `max_deletes_per_run` — per-tick cap.
- `dry_run` — default True.
