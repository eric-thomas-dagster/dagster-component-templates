# Warm Scheduled Job

**Isolated, always-warm compute that manages its own cron schedule in-process.** For scheduled jobs that need to start *instantly* the moment a schedule hits — no cold start, no minute-of-slop — while still running in dedicated, isolated resources rather than the shared code-location server.

## Why this exists

Dagster's native `ScheduleDefinition` mechanism cannot fire more precisely than **once a minute, by design**. Confirmed directly in the Dagster OSS scheduler source:

```python
def _get_next_scheduler_iteration_time(start_time: float) -> float:
    # Wait until at least the next minute to run again, since the minimum granularity
    # for a cron schedule is every minute
    last_minute_time = start_time - (start_time % SECONDS_IN_MINUTE)
    return last_minute_time + SECONDS_IN_MINUTE
```

Dagster+'s hosted control plane does **not** have a different, tighter implementation — its cloud daemon literally subclasses the same OSS `SchedulerDaemon` (`from dagster._daemon import ... SchedulerDaemon`), just wrapping it with Datadog/ddtrace instrumentation around the identical core loop. This is architectural, not a load artifact: no amount of idle agent capacity, no dedicated code-location server, changes that 1-minute floor — because the floor comes from how ticks are *detected* (a polling loop aligned to minute boundaries), not from how runs are *launched*.

On top of that floor, a normal scheduled run also pays launch latency every tick:
- **Isolated runs** (Dagster's default): a fresh container, up to ~3 minutes on Serverless.
- **Non-isolated runs**: faster (new process in an already-running code-location server), but still re-initializes resources from scratch each run, and is hard-capped at 0.25 vCPU / 1GB RAM on Serverless *regardless of how dedicated that server is* — that's a platform ceiling tied to non-isolated execution mode, not a symptom of sharing with other jobs.

This component sidesteps both problems by not being a schedule-triggered run at all:

1. One bounded, **genuinely isolated** run (dedicated resources — never tagged `dagster/isolation: disabled`, which would defeat the entire point).
2. `warmup_fn` runs exactly **once** per bounded run (load a model, open connections, JIT-compile — whatever the expensive part is).
3. The component parses `schedule` (an ordinary cron string) with `croniter` and manages its own tick timing — computing the exact next-fire instant and sleeping precisely until it, never asking any Dagster daemon to evaluate the cron on a polling interval.
4. `tick_fn` is called **directly as an in-process Python function call** at each precise tick — never through `RunRequest`, `dg api run launch`, `materialize()` against the instance, or any other Dagster run-launching API. That's what makes each tick instant: it's a function call in an already-warm process, not a new run.
5. Exits cleanly at `max_seconds` (same bounded-run pattern as `StreamingConsumerComponent`), so a paired health sensor relaunches — re-paying the warmup cost once per window, not once per tick.

## Quick example

```yaml
type: dagster_community_components.WarmScheduledJobComponent
attributes:
  asset_name: pricing_refresh_warm
  schedule: "*/15 * * * *"
  warmup_fn: "myproject.jobs.pricing:warmup"
  tick_fn: "myproject.jobs.pricing:run_tick"
  max_seconds: 3600
```

```python
# myproject/jobs/pricing.py
def warmup(context):
    model = load_pricing_model()   # expensive: runs once per hour, not every 15 min
    conn = open_warehouse_connection()
    return {"model": model, "conn": conn}

def run_tick(context, warm_state, scheduled_time):
    refresh_prices(warm_state["model"], warm_state["conn"])
    return {"rows_refreshed": 1234}   # optional -- merged into the tick's metadata
```

Pair with (unmodified — it only cares whether a run of the job is active, not what it does internally):

```yaml
type: dagster_community_components.StreamingRunHealthSensorComponent
attributes:
  sensor_name: pricing_refresh_health
  job_name: __ASSET_JOB
  asset_selection: [pricing_refresh_warm]
  minimum_interval_seconds: 30
  default_status: running
```

## `warmup_fn` / `tick_fn` — the dotted-path convention

Both are `"module.path:function_name"` strings (colon-separated — same convention as `dynamic_fanout_asset`'s `_resolve`), resolved via `importlib.import_module` at the **start of each materialize** (not at `build_defs`/component-load time — same convention as `dynamic_fanout_asset`, so validating/instantiating this component doesn't require your project's own modules to be importable, e.g. from a CI schema check against `example.yaml`). A typo still fails fast, before any warmup or tick work runs — just at run-start rather than component-load.

- `warmup_fn(context) -> Any` — optional. Called **once** per bounded run. Its return value (`warm_state`) is passed to every `tick_fn` call in that run.
- `tick_fn(context, warm_state, scheduled_time) -> Optional[dict]` — required. Called once per precise tick, as a plain synchronous function call. If it returns a dict, those keys are merged into that tick's `AssetMaterialization` metadata.

## Sub-minute schedules — a real capability, not just precise timing on the same schedule

Set `second_precision: true` to use 6-field cron (seconds-leading field, e.g. `"*/30 * * * * *"` for every 30 seconds). This isn't just "hitting a minute-granular schedule more precisely" — it's a schedule shape **Dagster's native scheduler cannot express at all**, since cron's finest native unit is the minute.

## Missed ticks — `catchup`

If the process is slow to start, or a previous `tick_fn` call overran into the next scheduled instant, the component is behind schedule. Default (`catchup: false`): skip straight to the next **future** tick — no pileup. Set `catchup: true` to execute every missed tick back-to-back instead, bounded by `max_catchup_ticks` (default 10) as a safety cap against a thundering-herd catch-up run.

## Tick failures — `tick_error_handling`

- `"continue"` (default): a `tick_fn` exception is caught, logged, and reported as a failed-tick `AssetMaterialization` (`status: failed` in metadata) — the loop continues to the next scheduled tick. A single transient failure doesn't force a full restart and re-paid warmup cost.
- `"raise"`: the exception propagates and fails the whole run immediately.

## Observability — drift is measured, not just asserted

Every tick's `AssetMaterialization` includes `drift_seconds` (actual fire time minus scheduled instant) and `tick_duration_seconds`. The run's final output metadata includes `max_drift_seconds` across the whole bounded run — so the precision this component buys you shows up directly in the Dagster+ catalog, not just in this README's claims.

## Multiple automations sharing ONE warm process — `jobs`

A single instance used to mean one schedule + one `tick_fn`. If you have several small, unrelated automations — each cheap per tick, but each paying its own full isolated-run allocation if given its own instance — set `jobs` instead of the flat `asset_name`/`schedule`/`tick_fn` fields:

```yaml
type: dagster_community_components.WarmScheduledJobComponent
attributes:
  warmup_fn: "myproject.automations.shared:warmup"  # ONE shared warmup --
    # e.g. returns {"hubspot": client, "quickbooks": client, "openai": client}
  max_seconds: 3600
  jobs:
    - asset_name: paid_invoice_sync
      schedule: "*/5 * * * *"
      tick_fn: "myproject.automations.invoices:check_and_sync"
    - asset_name: lead_followup_drafts
      schedule: "*/2 * * * *"
      tick_fn: "myproject.automations.leads:draft_followups"
```

`warmup_fn` stays a single, shared, top-level field — that's the actual compute saved: one warmup pass (e.g. opening the HubSpot/QuickBooks/OpenAI clients once) produces one `warm_state` that every job's `tick_fn` pulls from, instead of each automation separately re-opening its own connections. Internally this becomes a tiny merged scheduler: track each job's own next cron-tick instant, always fire whichever is soonest across all active jobs, recompute, repeat. Each job still gets its own `AssetKey`/materialization history (one `@multi_asset` spec per job), so unrelated automations stay independently visible in the catalog even though they share one process. Any per-job field omitted (`second_precision`/`timezone`/`catchup`/`max_catchup_ticks`/`tick_error_handling`/`description`/`group_name`/`asset_tags`/`kinds`/`owners`/`deps`) falls back to this component's matching top-level value.

**The real cost of sharing**: all jobs on one instance share ONE failure domain. If the shared process crashes, or a job's `tick_error_handling: raise` tick kills the run, EVERY job on that instance pauses together until the paired health sensor relaunches — not just the one that failed. Put automations that can tolerate correlated downtime together; keep anything truly independent-critical on its own instance. A job with `max_ticks` set stops firing (and drops out of the active rotation) once it hits that cap, while other jobs on the same instance keep running normally.

## Documenting the real cadence in the Schedules tab — `expose_informational_schedules`

By default (`true`), each job whose `schedule` is standard 5-field cron (not `second_precision`, which a real `ScheduleDefinition` cannot express at all) gets a real Dagster `ScheduleDefinition`, `default_status: STOPPED`, showing that job's actual cadence as living documentation. It is deliberately **not** wired to the real warm asset — it targets a trivial, separate no-op marker asset that does nothing but log a message. So even if someone flips it to RUNNING by mistake, or manually launches it, nothing of consequence happens: no duplicate automation run, no interference with the real warm process's own timing. Set `expose_informational_schedules: false` to suppress these entirely (e.g. to avoid Schedules-tab clutter). A `second_precision` job never gets one — showing a misleading minute-level approximation of a sub-minute cadence would be worse than showing nothing.

## Writing a `tick_fn` as a completely normal Dagster job — `@warmjob`

A bare `tick_fn` is a plain Python function call — instant, but with none of Dagster's own per-execution observability (no Runs-page entry, no per-step structured events, no `RetryPolicy`). `@warmjob` closes that gap:

```python
import dagster as dg
from dagster_community_components.assets.infrastructure.warm_scheduled_job.component import warmjob

@dg.op(required_resource_keys={"hubspot"})
def sync_paid_invoice(context):
    context.resources.hubspot.update_deal(...)

@warmjob          # apply AFTER @dg.job, so it wraps the resulting JobDefinition
@dg.job
def paid_invoice_sync():
    sync_paid_invoice()
```

```yaml
tick_fn: "myproject.automations.invoices:paid_invoice_sync"
```

Decorate a completely normal `@dg.job` (real `@op`s, real resources, real `RetryPolicy`) and reference it directly as a `tick_fn`. It dispatches via `job.execute_in_process(instance=context.instance, ...)` instead of through Dagster's run-launching path — because it's handed the REAL instance (not the ephemeral one `execute_in_process` defaults to), the dispatched execution is a genuine, separately-persisted run: visible on the Runs page, with real per-step events and retries, just without new-container launch latency (it's a synchronous call inside the already-warm process).

`warmup_fn`'s shared `warm_state`, when it's a dict, bridges straight into the dispatched job's normal resource system (`execute_in_process(resources=warm_state)`) — e.g. `warmup_fn` returns `{"hubspot": client, "quickbooks": client}`, and the wrapped job's ops just read `context.resources.hubspot` like any other Dagster job, with no idea they're being dispatched from inside a warm process.

Real caveats:
- `execute_in_process` forces the in-process executor (no multiprocess/k8s executor), and the default io_manager falls back to in-memory unless you pass real resources — fine for most integration-glue jobs, worth knowing for anything disk/warehouse-IO-manager-dependent.
- More per-call overhead than a bare `tick_fn` function call (it builds/validates a real execution plan each time) — use `@warmjob` when you want per-execution observability/retries; use a bare `tick_fn` for truly hyper-frequent sub-second ticks.
- A failed dispatched job raises, which flows into this component's existing `tick_error_handling` (`continue`/`raise`) — no separate error-handling scheme to learn.
- Same shared-failure-domain tradeoff as `jobs` above: still dispatched from inside one shared warm process.

Optional config as decorator kwargs: `@warmjob(tags={"team": "finance"})`, plus `run_config`/`run_config_fn(warm_state, scheduled_time)`, `tags_fn(warm_state, scheduled_time)`, `op_selection`, `asset_selection` — all passed straight through to `execute_in_process`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Single-job mode only: output asset name (also used by the paired health sensor's asset_selection). Required when `jobs` is not set. |
| `group_name` | `str` | — | Component-level default group_name; a `jobs` entry may override with its own `group_name`. |
| `description` | `str` | — | Single-job mode only (per-job override available in `jobs` entries). |
| `asset_tags` | `Dict[str, str]` | — | Component-level default asset_tags; a `jobs` entry may override with its own `asset_tags`. |
| `kinds` | `List[str]` | — | Component-level default kinds; a `jobs` entry may override with its own `kinds`. |
| `owners` | `List[str]` | — | Component-level default owners; a `jobs` entry may override with its own `owners`. |
| `deps` | `List[str]` | — | Component-level default deps; a `jobs` entry may override with its own `deps`. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `schedule` | `str` | — | Single-job mode only: a cron string. Standard 5-field (e.g. '*/15 * * * *' for every 15 minutes) by default. Evaluated by this component's own internal loop via `croniter` — this is NOT a Dagster ScheduleDefinition and i… _(full docs in schema.json + component README)_ |
| `second_precision` | `bool` | `false` | Single-job mode only: parse `schedule` as 6-field cron (seconds-leading) instead of standard 5-field. |
| `tick_fn` | `str` | — | Single-job mode only: 'module.path:function_name', called once per precise scheduled tick as `tick_fn(context, warm_state, scheduled_time) -> Optional[dict]`. Required when `jobs` is not set. |
| `max_ticks` | `int` | — | Single-job mode only: optional cap on total ticks executed before exit (in addition to max_seconds). |
| `catchup` | `bool` | `false` | Single-job mode only (per-job override available in `jobs` entries). If a tick's scheduled instant has already passed by the time the loop checks again, default (false) skips straight to the next FUTURE tick. Set true to… _(full docs in schema.json + component README)_ |
| `max_catchup_ticks` | `int` | `10` | Single-job mode only (per-job override available in `jobs` entries): safety cap on consecutive missed ticks executed back-to-back when catchup=true. |
| `tick_error_handling` | `str` | `"continue"` | Single-job mode only (per-job override available in `jobs` entries). 'continue' (default): a tick_fn exception is logged + reported as a failed-tick materialization, and the loop continues — a single tick's transient fai… _(full docs in schema.json + component README)_ |
| `jobs` | `List[Dict[str, Any]]` | — | Multi-job mode: a list of independently-scheduled automations sharing this ONE warm process and ONE `warmup_fn` pass. Each entry: {asset_name, schedule, tick_fn} required; optional per-entry overrides for second_precisio… _(full docs in schema.json + component README)_ |
| `expose_informational_schedules` | `bool` | `true` | Emit a real, default-STOPPED Dagster ScheduleDefinition per job whose schedule is standard 5-field cron (not second_precision), purely so the real cadence is visible in the Schedules tab. It targets a trivial no-op asset… _(full docs in schema.json + component README)_ |
| `warmup_fn` | `str` | — | 'module.path:function_name', called ONCE per bounded run lifetime (not per tick, and shared across every job in `jobs` if set) as `warmup_fn(context) -> Any`. Do the expensive setup here (load a model, open connections)… _(full docs in schema.json + component README)_ |
| `timezone` | `str` | `"UTC"` | IANA timezone name the cron string(s) are evaluated in. Component-level default; a `jobs` entry may override with its own `timezone`. |
| `max_seconds` | `int` | `3600` | Bounded run duration in seconds for the WHOLE process (shared across every job in `jobs`), same pattern as StreamingConsumerComponent. Set LESS than your Dagster+ Serverless per-run timeout. `null` for a truly unbounded… _(full docs in schema.json + component README)_ |
| `op_name` | `str` | — | Multi-job mode only: the underlying op's name (defaults to 'warm_scheduled_jobs_multi'). Set this if you have more than one multi-job WarmScheduledJobComponent instance in the same code location, to avoid an op-name collision. |

[//]: # (FIELDS:END)

## What this does NOT give you

- **No Schedules-tab entry, no visible "next tick" in the Dagster+ UI.** There is no real `ScheduleDefinition` here, on purpose. If catalog visibility of the cron matters to your team, surface the `schedule` value separately (e.g. in the asset's `description`) — don't wire up a cosmetic, non-functional `ScheduleDefinition` purely for display, since a schedule that never actually fires reads as broken in the UI.
- **Isolation, not compute elasticity.** This is one long-lived process with whatever resources you provision for it — it doesn't scale out horizontally per tick the way N separate scheduled runs conceptually could. If your ticks need to run in parallel against each other (not just back-to-back), this component's single-process model isn't the right fit.

## When to reach for this vs. other components

- **`streaming_run_health_sensor` + a normal `ScheduleDefinition`** — fine when a 1-minute-or-worse trigger floor and normal run-launch latency are both acceptable. Most scheduled jobs.
- **`streaming_consumer`** — continuous consumption from a queue (Kafka/etc). No scheduling concept at all; the loop is driven by "is there a message," not "is it time."
- **`warm_scheduled_job`** — the schedule itself needs sub-minute precision, or the job needs genuinely isolated/dedicated resources AND zero cold-start latency on every tick. The narrower, more demanding case.

## Requirements

- `dagster`
- `croniter`

## Validation

`validation.level: code`. Live-verified (real `croniter`, real wall-clock sleeps, nothing mocked): warmup runs exactly once per bounded run and every tick observes the identical `warm_state` object; measured drift against the scheduled instant is sub-500ms; `tick_error_handling: continue` survives a failing tick and keeps looping (`raise` correctly fails the whole run); `max_seconds` and `max_ticks` both correctly bound the loop; a malformed cron string, a 6-field string without `second_precision: true` (croniter's own `is_valid()` is too lenient to catch this alone — confirmed live it accepts 6-field and even 7-field strings regardless of the `second_at_beginning` flag, so this component enforces its own strict field-count check first), an invalid dotted callable path, and an invalid timezone are all rejected with clear errors at `build_defs` time. Not yet verified: multi-hour-boundary DST transitions in non-UTC timezones, or behavior under `catchup: true` with a very large missed-tick backlog beyond `max_catchup_ticks`.
