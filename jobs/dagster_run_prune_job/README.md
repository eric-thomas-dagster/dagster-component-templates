# DagsterRunPruneJobComponent

Op-shaped job that deletes old Dagster runs (and their event log entries)
beyond a configurable retention window. Every prod deployment eventually
needs this — the event log grows unbounded and sqlite/postgres storage
bloats.

## Behavior

1. Every tick (default: daily at 4am), queries runs updated before `max_age_days`.
2. Groups candidates by `job_name`; for each job, preserves the `keep_last_n_per_job` most-recent runs as a safety net (accounts for recent runs already outside the candidate window).
3. Deletes the remainder via `instance.delete_run(run_id)` — cascades to event log entries.
4. Hard-capped by `max_deletes_per_run` per tick; older stragglers picked up next tick.

## YAML example

```yaml
type: dagster_component_templates.DagsterRunPruneJobComponent
attributes:
  job_name: prune_dagster_run_history
  schedule: "0 4 * * *"                # daily at 4am
  default_status: RUNNING
  max_age_days: 30
  keep_last_n_per_job: 10               # safety net
  only_terminal_states: true            # SUCCESS/FAILURE/CANCELED only
  max_deletes_per_run: 1000
  dry_run: false
```

## Options

- `max_age_days` — retention window. Default 30.
- `keep_last_n_per_job` — always preserve N most-recent runs per job. Default 10.
- `only_terminal_states` — safer default (never prune in-flight runs).
- `tags_filter` — scope to specific tag key/values.
- `max_deletes_per_run` — per-tick cap. Default 1000.
- `dry_run` — log the plan without deleting.
