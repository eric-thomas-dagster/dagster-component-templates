# StuckRunTerminatorJobComponent

Op-shaped job that finds Dagster runs stuck in `STARTING`/`STARTED`/`CANCELING`
longer than a threshold and force-cancels them (or marks them as `FAILURE`
for alerting). Every prod deployment eventually hits this — a K8s pod OOMs
mid-run, a worker container gets evicted, a network partition — and the
run status stays "RUNNING" forever until an operator manually clears it.

## Behavior

1. Every tick (default: 15min), queries runs in in-flight statuses.
2. Compares `start_time` (falls back to create timestamp) against `stuck_after_hours`.
3. For each candidate:
   - Emits an engine event so the run's timeline shows why the status changed.
   - Calls `report_run_canceling(run)` + `report_run_canceled(run)` (or `report_run_failed(run)` if `terminal_status=FAILURE`).
4. Returns a summary: `checked`, `stuck_found`, `terminated`.

Note: this changes the run's STATUS. It does NOT terminate the underlying
worker process — that depends on your run launcher. K8s launcher will
clean up the pod when it notices the status change; Docker/Celery/etc.
have their own behaviors. This job's job is to un-stick the DASHBOARD.

## YAML example

```yaml
type: dagster_component_templates.StuckRunTerminatorJobComponent
attributes:
  job_name: terminate_stuck_runs
  schedule: "*/15 * * * *"
  default_status: RUNNING
  stuck_after_hours: 6
  terminal_status: CANCELED       # or FAILURE
  dry_run: false
```

## Options

- `stuck_after_hours` — threshold. Default 6h.
- `terminal_status` — `CANCELED` (clean) or `FAILURE` (triggers alerting).
- `tags_filter` — only inspect runs matching specific tags.
- `dry_run` — list stuck runs without acting.
- `scan_limit` — max runs to inspect per tick.
