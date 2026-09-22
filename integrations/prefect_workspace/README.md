# PrefectWorkspaceComponent

Auto-discover every Prefect deployment in an instance and emit one triggerable Dagster asset per deployment — no hand-written `prefect_flow_run` block per deployment. `StateBackedComponent` — discovery cached to disk, so no Prefect API calls fire at Dagster defs-load time.

**Why this exists**: `prefect_flow_run` requires one YAML block per deployment — a real tax for "just show me everything already deployed in my Prefect instance." This component removes that tax the same way `mlflow_workspace` / `snowflake_workspace` do for their own targets.

Materializing a discovered deployment's asset triggers that deployment via `run_deployment` and (by default) waits for it to reach a terminal state, with `prefect_flow_run`'s **full** observability applied uniformly across every discovered deployment: `forward_termination`, `stream_logs`, `stream_artifacts` all work exactly the same way here as they do there — no reduced "lean" version. `check_names` and Dagster-side scheduling are the two things that do NOT apply globally — see below for why, and how to opt one deployment into either via `assets_by_name`.

**Requires Prefect 3.0+.** Same constraint as every other component in this vendor (verified against Prefect 2.20's actual client schemas, not assumed) — see `vendors/prefect.md` for specifics.

## check_names and scheduling are per-deployment, not global — on purpose

**Prefect has no native asset-check concept at all** — verified directly: `prefect.assets` exposes only `Asset`/`AssetProperties`/`materialize`/`add_asset_metadata`, and the client has zero methods with "check" in the name. `check_names` is a convention this registry invented on top of Prefect's generic, untyped artifacts — Prefect itself has no idea any given artifact means "check" rather than "just some info the flow logged." There's nothing to auto-discover and nothing to exclude from, because there's no source of truth for "which of my deployments have checks" anywhere in Prefect's own model. A global `check_names` list would require literally every discovered deployment to implement that exact artifact convention — Dagster hard-fails a declared check that never gets a result, so this would break every deployment that doesn't happen to match.

Scheduling has a different reason to stay explicit: it would be technically possible to infer "Dagster should own this deployment's schedule" from Prefect's own `active` flag on that deployment's `DeploymentSchedule` (pause it in Prefect → hand it to Dagster). We deliberately don't wire it that way — pausing a schedule is a side signal someone could flip for an unrelated reason (debugging, maintenance) and silently start Dagster firing on a cadence nobody asked it to own.

Both are opt-in via **`assets_by_name`**, keyed by `flow_name/deployment_name` (same string `deployment_selector` matches against):

```yaml
assets_by_name:
  nightly-report/prod:
    check_names: [row_count_check]   # matches artifact key "row-count-check"
    schedule: true
```

- `check_names` here requires `stream_artifacts: true` globally (validated at build time) — that's the only way a check result is ever collected.
- `schedule: true` here requires `auto_schedule: true` globally (the master switch), AND that this specific deployment's own Prefect cron schedule is already paused — `build_defs_from_state` **raises** if you opt a deployment in while its Prefect-side schedule is still active, since both would fire it on the same tick otherwise. Pause it in Prefect first.
- The standard `@asset`-kwarg overrides also work here: `key`, `group_name`, `description`, `deps`, `metadata`, `tags`, `kinds`, `owners` — identical merge semantics to `SnowflakeWorkspaceComponent.assets_by_name` / the official `dagster-databricks` `assets_by_task_key` pattern.

## Observing completions from outside Dagster

`polling_sensor: true` (default off) adds a sensor watching *all* discovered deployments for flow runs entering a terminal state, and emits an `AssetObservation` on the matching deployment's asset — the auto-discovery equivalent of hand-configuring `prefect_flow_run_sensor` once per deployment. Useful when some deployments are triggered by Prefect's own schedule or other application code, not just by Dagster.

Matches on `FlowRun.deployment_id` (a UUID) — verified against the live API that this is the only deployment linkage a flow run carries; there's no inline `flow_name/deployment_name` string on the run itself, so the cached discovery state stores each deployment's id specifically so the sensor can key off it directly.

## Related

- [`prefect_flow_run`](../../assets/infrastructure/prefect_flow_run) — one deployment, fully configurable (`stream_logs`, `stream_artifacts`, `check_names`, `execution_mode: pipes`).
- [`prefect_flow_run_sensor`](../../sensors/prefect_flow_run_sensor) — the single-deployment equivalent of this component's `polling_sensor`.
- [`prefect_background_task`](../../assets/infrastructure/prefect_background_task) — trigger a single `@task` via `.delay()` instead of a deployment.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `workspace` | `PrefectWorkspaceResource` | Prefect connection as a PrefectWorkspaceResource (api_url + optional api_key_env_var + ui_url). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `int` | — | — |
| `poll_interval_seconds` | `float` | `5.0` | — |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Group name for all imported assets. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `stream_logs` | `bool` | `false` | Forward each deployment's own Prefect logs into the Dagster run log while waiting, via read_logs — no shared filesystem or blob store required. Applies uniformly to every discovered deployment. Off by default. Same mecha… _(full docs in schema.json + component README)_ |
| `stream_artifacts` | `bool` | `false` | Forward Prefect artifacts each deployment's flow creates as AssetObservation events. Applies uniformly to every discovered deployment. Off by default. Required (validated at build time) if any assets_by_name entry sets c… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `translation` | `TranslationFn[PrefectWorkspaceObjectProps]` | — | Function used to translate deployment properties into Dagster asset specs. Called for each discovered deployment. If unset, the base translator's default AssetSpec is used. |
| `deployment_selector` | `PrefectDeploymentSelector` | — | Optional inclusion/exclusion filter on 'flow_name/deployment_name' strings. |
| `wait_for_result` | `bool` | `true` | Wait for each triggered flow run to reach a terminal state before materializing. |
| `fail_on_flow_run_failure` | `bool` | `true` | — |
| `forward_termination` | `bool` | `true` | Cancel the Prefect flow run if the Dagster run is terminated while waiting — same mechanism as prefect_flow_run.forward_termination. |
| `assets_by_name` | `Dict[str, Dict[str, Any]]` | — | Per-deployment overrides, keyed by 'flow_name/deployment_name' (same string deployment_selector matches against). Mirrors SnowflakeWorkspaceComponent.assets_by_name / the official dagster-databricks assets_by_task_key pa… _(full docs in schema.json + component README)_ |
| `auto_schedule` | `bool` | `false` | Master switch for Dagster-side scheduling — even when True, a deployment only gets a Dagster ScheduleDefinition if it's ALSO explicitly named in assets_by_name.<flow>/<deployment>.schedule: true. Deliberately NOT inferre… _(full docs in schema.json + component README)_ |
| `asset_key_prefix` | `List[str]` | `lambda: ['prefect']()` | Key prefix used for all emitted AssetKeys. |
| `compute_kind` | `str` | `"prefect"` | Compute kind tag for all imported assets. |
| `poll_interval_seconds_sensor` | `int` | `60` | Minimum seconds between polling-sensor evaluations. Only consulted when polling_sensor: true. |
| `generate_sensor` | `bool` | `false` | If true, adds a sensor watching ALL discovered deployments for flow runs entering a terminal state and emits an AssetObservation on the matching deployment's asset. Use when some deployments are triggered outside Dagster… _(full docs in schema.json + component README)_ |
| `defs_state` | `ResolvedDefsStateConfig` | `DefsStateConfigArgs.local_filesystem()` | State backend for cached workspace discovery. Local filesystem by default. |

[//]: # (FIELDS:END)
