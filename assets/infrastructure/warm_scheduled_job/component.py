"""WarmScheduledJobComponent — an isolated, always-warm process that manages
its own cron schedule(s) internally, bypassing Dagster's SchedulerDaemon
entirely.

## Why this exists

Dagster's native `ScheduleDefinition` mechanism cannot fire more precisely
than once-a-minute, by design — confirmed directly in the OSS scheduler
source (`_get_next_scheduler_iteration_time`): "Wait until at least the next
minute to run again, since the minimum granularity for a cron schedule is
every minute." Dagster+'s hosted control plane does not have a different,
tighter-precision implementation — its cloud daemon literally subclasses the
same OSS `SchedulerDaemon` class, just adding Datadog/ddtrace instrumentation
around the identical core loop. This is architectural, not a load artifact:
no amount of idle agent capacity or dedicated code-location-server resources
changes that 1-minute floor, because the floor comes from how ticks are
*detected*, not from how runs are *launched*.

On top of that floor, a normal scheduled run (isolated OR non-isolated) also
pays *launch* latency every single tick: isolated runs provision a fresh
container (up to ~3 minutes on Serverless); non-isolated runs spin a new
process in the shared code-location server (much faster, but still
re-initializes resources from scratch every run, and is hard-capped at
0.25 vCPU / 1GB RAM on Serverless regardless of how dedicated that server is).

This component sidesteps both problems by not being a schedule-triggered run
at all. It's a single, bounded, ISOLATED run (dedicated resources — the
default isolation mode; never tag it `dagster/isolation: disabled`, which
would defeat the whole point) that:

  1. Runs `warmup_fn` exactly ONCE per bounded run lifetime (load a model,
     open connections, JIT-compile, whatever the expensive part is).
  2. Parses each job's `schedule` (an ordinary cron string) with `croniter`
     and manages its OWN tick timing — computing the exact next-fire instant
     and sleeping precisely until it, rather than asking any Dagster daemon
     to evaluate the cron on a polling interval.
  3. Calls that job's `tick_fn(context, warm_state, scheduled_time)` directly
     as an in-process Python function call at each tick — never through
     Dagster's run-launching APIs (no `RunRequest`, no `dg api run launch`,
     no `materialize()` against the instance). That's what makes each tick
     truly instant: it's a function call in an already-warm process, not a
     new run.
  4. Emits an `AssetMaterialization` per tick, keyed to that specific job's
     own asset (with measured drift against the scheduled instant, so the
     precision this buys you is directly observable in the catalog, not just
     asserted) — this is the one place Dagster's own machinery is still used
     for the real automation, and it's optional bookkeeping, not the trigger
     mechanism.
  5. Exits cleanly at `max_seconds` (same bounded-run pattern as
     `StreamingConsumerComponent`) so a paired health sensor can relaunch —
     re-paying the warmup cost once per `max_seconds` window, not once per
     tick. If your schedule fires every 15 minutes and warmup takes 45s,
     `max_seconds: 3600` pays that 45s once an hour, not 4x an hour.

## Multiple independently-scheduled automations sharing ONE warm process

A single instance used to mean one schedule + one `tick_fn`. If you have
several small, unrelated automations (each cheap per tick, but each paying
its own full isolated-run allocation if given its own instance), set `jobs:`
instead of the flat `asset_name`/`schedule`/`tick_fn` fields — a list of
`{asset_name, schedule, tick_fn, ...}` dicts, each with the same per-job
options the flat fields support (any omitted per-job option falls back to
this component's own top-level value, e.g. `timezone`/`catchup`/
`tick_error_handling`). `warmup_fn` stays a SINGLE, shared, top-level field —
that's the actual compute saved: one warmup pass produces one `warm_state`
(e.g. a dict of clients: `{"hubspot": ..., "quickbooks": ..., "openai": ...}`)
that every registered job's `tick_fn` can pull from, instead of each
automation separately re-opening its own connections in its own process.

Internally this becomes a tiny merged scheduler: track each job's own next
cron-tick instant, always sleep until the SOONEST one across all active jobs,
fire that job's `tick_fn`, recompute its next tick, repeat — each job still
gets its OWN `AssetKey`/materialization history (one `@multi_asset` spec per
job), so unrelated automations stay independently visible/observable in the
catalog even though they share one process.

**The real cost of sharing**: all jobs on one instance share ONE failure
domain. If the shared process crashes, or a job's `tick_error_handling:
raise` tick kills the run, EVERY job on that instance pauses together until
the paired health sensor relaunches — not just the one that failed. That's
the actual price of sharing a warm allocation across many automations
(the same tradeoff n8n's own single-runtime model has). Put automations that
can tolerate correlated downtime together; keep anything truly
independent-critical on its own instance.

A job with `max_ticks` set stops firing (and is dropped from the active
rotation) once it hits that cap, while OTHER jobs on the same instance keep
running normally until `max_seconds` or their own caps.

## Pairing for auto-restart

Reuse `StreamingRunHealthSensorComponent` UNCHANGED — it only cares about
"is a run of this job currently active," not what the job's compute does
internally, so it works for this component exactly as built for
`StreamingConsumerComponent`:

    type: dagster_community_components.StreamingRunHealthSensorComponent
    attributes:
      sensor_name: pricing_refresh_health
      job_name: __ASSET_JOB
      asset_selection: [pricing_refresh_warm]
      minimum_interval_seconds: 30
      default_status: running

## Informational (non-firing) Schedules-tab entries

By default (`expose_informational_schedules: true`), each job whose
`schedule` is a STANDARD 5-field cron (not `second_precision`, which a real
Dagster `ScheduleDefinition` cannot express at all) gets a real
`ScheduleDefinition` in the Schedules tab, `default_status=STOPPED`, showing
that job's actual cadence as living documentation. It is deliberately NOT
wired to the real warm asset — it targets a trivial, separate no-op asset/job
that does nothing but log an informational message. This means even if
someone flips it to RUNNING by mistake (or manually launches it), nothing of
consequence happens: no duplicate automation run, no interference with the
real warm process's own internal timing. It exists purely so the real
cadence is visible without a dedicated UI feature for "process-internal,
non-Dagster-native schedules." Set `expose_informational_schedules: false`
to suppress these entirely (e.g. to avoid Schedules-tab clutter).

A job using `second_precision` (6-field, sub-minute cadence) never gets one
of these — a real `ScheduleDefinition`'s cron cannot express sub-minute
cadence at all, and showing a misleading minute-level approximation would be
worse than showing nothing.

## What this does NOT give you

- No Dagster Schedules-tab entry for the REAL execution mechanism — the
  informational schedule (see above) documents the cadence, it does not
  control or trigger it. If a job's `second_precision` is set, there is no
  Schedules-tab entry for it at all (see above for why).
- No catch-up-by-default for missed ticks (e.g. if the process was slow to
  start, or a previous `tick_fn` overran into the next scheduled instant):
  default behavior skips to the next future tick rather than piling up
  back-to-back catch-up executions. Set `catchup: true` (with
  `max_catchup_ticks` as a safety cap) if you need every tick to execute.
"""
import importlib
import time
from datetime import datetime, timedelta, timezone as _dt_timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _resolve(callable_path: str, field_name: str):
    """Resolve a `module.path:function_name` reference to the actual
    callable. Same convention as dynamic_fanout_asset/component.py's
    `_resolve` — colon-separated, not dotted, so a dotted module path
    (`myproject.jobs.pricing`) is unambiguous from the function name."""
    if ":" not in callable_path:
        raise ValueError(
            f"warm_scheduled_job: {field_name}={callable_path!r} must be "
            f"'module.path:function_name' (colon-separated), e.g. "
            f"'myproject.jobs.pricing:warmup'."
        )
    module_path, fn_name = callable_path.split(":", 1)
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"warm_scheduled_job: {field_name}: cannot import module {module_path!r}: {e}") from e
    try:
        return getattr(mod, fn_name)
    except AttributeError as e:
        raise AttributeError(f"warm_scheduled_job: {field_name}: {module_path!r} has no attribute {fn_name!r}") from e


def _next_tick(cron_iter, base: datetime) -> datetime:
    """Advance a croniter to strictly after `base` and return the next tick."""
    return cron_iter.get_next(datetime)


def _validate_cron(schedule_str: str, second_precision: bool, who: str) -> None:
    """Shared cron-shape validation, parameterized by a `who` label (e.g. a
    job's asset_name) so a multi-job config's error points at the right
    entry. Field-count enforced ourselves before croniter.is_valid() --
    confirmed live that croniter.is_valid() is too lenient to be the sole
    gate (it accepts 6-field strings even when second_at_beginning=False,
    and even accepts a malformed 7-field string as "valid")."""
    from croniter import croniter

    expected_fields = 6 if second_precision else 5
    actual_fields = len(schedule_str.split())
    if actual_fields != expected_fields:
        raise ValueError(
            f"warm_scheduled_job: {who}: schedule={schedule_str!r} has {actual_fields} fields, "
            f"expected exactly {expected_fields} ({'6-field second-precision' if second_precision else '5-field standard'} cron). "
            f"{'Set second_precision: true for a seconds-leading field.' if actual_fields == 6 and not second_precision else ''}"
        )
    if not croniter.is_valid(schedule_str, second_at_beginning=second_precision):
        raise ValueError(
            f"warm_scheduled_job: {who}: schedule={schedule_str!r} is not a valid "
            f"{'6-field second-precision' if second_precision else '5-field'} cron string."
        )


class WarmScheduledJobComponent(Component, Model, Resolvable):
    """An isolated, always-warm job that manages one or more precise cron
    schedules in-process instead of relying on Dagster's minute-granular
    SchedulerDaemon. See the module docstring for the full rationale.

    Single-job example:
        ```yaml
        type: dagster_community_components.WarmScheduledJobComponent
        attributes:
          asset_name: pricing_refresh_warm
          schedule: "*/15 * * * *"
          warmup_fn: "myproject.jobs.pricing:warmup"
          tick_fn: "myproject.jobs.pricing:run_tick"
          max_seconds: 3600
        ```

    Multi-job example (several automations sharing ONE warm process/warmup):
        ```yaml
        type: dagster_community_components.WarmScheduledJobComponent
        attributes:
          warmup_fn: "myproject.automations.shared:warmup"  # builds {"hubspot":..., "quickbooks":..., "openai":...}
          max_seconds: 3600
          jobs:
            - asset_name: paid_invoice_sync
              schedule: "*/5 * * * *"
              tick_fn: "myproject.automations.invoices:check_and_sync"
            - asset_name: lead_followup_drafts
              schedule: "*/2 * * * *"
              tick_fn: "myproject.automations.leads:draft_followups"
        ```
    """

    # ── Legacy single-job flat fields (mutually exclusive with `jobs`) ────
    asset_name: Optional[str] = Field(
        default=None,
        description="Single-job mode only: output asset name (also used by the paired health sensor's asset_selection). Required when `jobs` is not set.",
    )
    schedule: Optional[str] = Field(
        default=None,
        description=(
            "Single-job mode only: a cron string. Standard 5-field (e.g. "
            "'*/15 * * * *' for every 15 minutes) by default. Evaluated by "
            "this component's own internal loop via `croniter` — this is NOT "
            "a Dagster ScheduleDefinition and is never handed to the "
            "SchedulerDaemon. Set `second_precision: true` to use 6-field "
            "cron (seconds-leading, e.g. '*/30 * * * * *'). Required when "
            "`jobs` is not set."
        ),
    )
    second_precision: bool = Field(
        default=False,
        description="Single-job mode only: parse `schedule` as 6-field cron (seconds-leading) instead of standard 5-field.",
    )
    tick_fn: Optional[str] = Field(
        default=None,
        description=(
            "Single-job mode only: 'module.path:function_name', called once "
            "per precise scheduled tick as `tick_fn(context, warm_state, "
            "scheduled_time) -> Optional[dict]`. Required when `jobs` is not set."
        ),
    )
    max_ticks: Optional[int] = Field(
        default=None,
        description="Single-job mode only: optional cap on total ticks executed before exit (in addition to max_seconds).",
    )
    catchup: bool = Field(
        default=False,
        description=(
            "Single-job mode only (per-job override available in `jobs` "
            "entries). If a tick's scheduled instant has already passed by "
            "the time the loop checks again, default (false) skips straight "
            "to the next FUTURE tick. Set true to execute every missed tick "
            "back-to-back instead (bounded by max_catchup_ticks)."
        ),
    )
    max_catchup_ticks: int = Field(
        default=10,
        description="Single-job mode only (per-job override available in `jobs` entries): safety cap on consecutive missed ticks executed back-to-back when catchup=true.",
    )
    tick_error_handling: str = Field(
        default="continue",
        description=(
            "Single-job mode only (per-job override available in `jobs` "
            "entries). 'continue' (default): a tick_fn exception is logged + "
            "reported as a failed-tick materialization, and the loop "
            "continues — a single tick's transient failure doesn't force a "
            "full restart. 'raise': the exception propagates and fails the "
            "WHOLE run immediately (and, in multi-job mode, every job "
            "sharing this instance stops with it)."
        ),
    )

    # ── Multi-job mode ──────────────────────────────────────────────────
    jobs: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-job mode: a list of independently-scheduled automations "
            "sharing this ONE warm process and ONE `warmup_fn` pass. Each "
            "entry: {asset_name, schedule, tick_fn} required; optional "
            "per-entry overrides for second_precision/timezone/max_ticks/"
            "catchup/max_catchup_ticks/tick_error_handling/description/"
            "group_name/asset_tags/kinds/owners/deps — any field omitted on "
            "an entry falls back to this component's matching top-level "
            "value. Mutually exclusive with the flat asset_name/schedule/"
            "tick_fn fields (set exactly one of `jobs` or `tick_fn`)."
        ),
    )
    expose_informational_schedules: bool = Field(
        default=True,
        description=(
            "Emit a real, default-STOPPED Dagster ScheduleDefinition per job "
            "whose schedule is standard 5-field cron (not second_precision), "
            "purely so the real cadence is visible in the Schedules tab. It "
            "targets a trivial no-op asset, never the real warm automation — "
            "flipping it to RUNNING or manually launching it does nothing "
            "harmful. Set false to suppress these entirely."
        ),
    )

    # ── Shared across all jobs (single- or multi-job mode) ──────────────
    warmup_fn: Optional[str] = Field(
        default=None,
        description=(
            "'module.path:function_name', called ONCE per bounded run "
            "lifetime (not per tick, and shared across every job in `jobs` "
            "if set) as `warmup_fn(context) -> Any`. Do the expensive setup "
            "here (load a model, open connections) — the return value is "
            "passed as `warm_state` to every job's `tick_fn` call in this "
            "run. Optional: omit if there's nothing to warm up."
        ),
    )
    timezone: str = Field(
        default="UTC",
        description="IANA timezone name the cron string(s) are evaluated in. Component-level default; a `jobs` entry may override with its own `timezone`.",
    )
    max_seconds: Optional[int] = Field(
        default=3600,
        description=(
            "Bounded run duration in seconds for the WHOLE process (shared "
            "across every job in `jobs`), same pattern as "
            "StreamingConsumerComponent. Set LESS than your Dagster+ "
            "Serverless per-run timeout. `null` for a truly unbounded loop — "
            "bounded is safer: graceful exit + summary metadata, then the "
            "paired health sensor launches the next run."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Component-level default group_name; a `jobs` entry may override with its own `group_name`.")
    description: Optional[str] = Field(default=None, description="Single-job mode only (per-job override available in `jobs` entries).")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Component-level default asset_tags; a `jobs` entry may override with its own `asset_tags`.")
    kinds: Optional[List[str]] = Field(default=None, description="Component-level default kinds; a `jobs` entry may override with its own `kinds`.")
    owners: Optional[List[str]] = Field(default=None, description="Component-level default owners; a `jobs` entry may override with its own `owners`.")
    deps: Optional[List[str]] = Field(default=None, description="Component-level default deps; a `jobs` entry may override with its own `deps`.")
    op_name: Optional[str] = Field(
        default=None,
        description="Multi-job mode only: the underlying op's name (defaults to 'warm_scheduled_jobs_multi'). Set this if you have more than one multi-job WarmScheduledJobComponent instance in the same code location, to avoid an op-name collision.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Isolated, always-warm job that manages one or more precise cron schedules in-process."

    def _normalize_job_specs(self) -> List[Dict[str, Any]]:
        """Merge legacy flat fields and `jobs` into one list of fully-resolved
        per-job dicts, applying component-level fallbacks for any field a
        `jobs` entry doesn't set. Raises on invalid mode combinations."""
        have_jobs = bool(self.jobs)
        have_legacy = self.tick_fn is not None

        if have_jobs and have_legacy:
            raise ValueError(
                "warm_scheduled_job: set exactly one of `jobs` (multi-job mode) "
                "or `tick_fn`/`asset_name`/`schedule` (single-job legacy mode), not both."
            )
        if not have_jobs and not have_legacy:
            raise ValueError(
                "warm_scheduled_job: must set either `jobs` (multi-job mode) "
                "or `tick_fn` + `asset_name` + `schedule` (single-job mode)."
            )

        if have_legacy:
            if not self.asset_name:
                raise ValueError("warm_scheduled_job: `asset_name` is required in single-job mode.")
            if not self.schedule:
                raise ValueError("warm_scheduled_job: `schedule` is required in single-job mode.")
            raw_jobs = [{
                "asset_name": self.asset_name,
                "schedule": self.schedule,
                "second_precision": self.second_precision,
                "tick_fn": self.tick_fn,
                "max_ticks": self.max_ticks,
                "catchup": self.catchup,
                "max_catchup_ticks": self.max_catchup_ticks,
                "tick_error_handling": self.tick_error_handling,
                "description": self.description,
                "group_name": self.group_name,
                "asset_tags": self.asset_tags,
                "kinds": self.kinds,
                "owners": self.owners,
                "deps": self.deps,
                "timezone": self.timezone,
            }]
        else:
            raw_jobs = self.jobs or []
            if not raw_jobs:
                raise ValueError("warm_scheduled_job: `jobs` must be a non-empty list.")

        specs = []
        for idx, raw in enumerate(raw_jobs):
            name = raw.get("asset_name")
            if not name:
                raise ValueError(f"warm_scheduled_job: jobs[{idx}]: `asset_name` is required.")
            schedule_str = raw.get("schedule")
            if not schedule_str:
                raise ValueError(f"warm_scheduled_job: jobs[{idx}] ({name}): `schedule` is required.")
            tick_path = raw.get("tick_fn")
            if not tick_path:
                raise ValueError(f"warm_scheduled_job: jobs[{idx}] ({name}): `tick_fn` is required.")

            second_precision = raw.get("second_precision", False)
            tick_error_handling = raw.get("tick_error_handling", self.tick_error_handling)
            if tick_error_handling not in ("continue", "raise"):
                raise ValueError(
                    f"warm_scheduled_job: jobs[{idx}] ({name}): tick_error_handling must be "
                    f"'continue' or 'raise', got {tick_error_handling!r}."
                )

            tz_name = raw.get("timezone", self.timezone)
            try:
                from zoneinfo import ZoneInfo
                tzinfo = ZoneInfo(tz_name)
            except Exception as e:
                raise ValueError(f"warm_scheduled_job: jobs[{idx}] ({name}): invalid timezone {tz_name!r}: {e}") from e

            _validate_cron(schedule_str, second_precision, who=f"jobs[{idx}] ({name})")

            specs.append({
                "asset_name": name,
                "asset_key": AssetKey.from_user_string(name),
                "schedule": schedule_str,
                "second_precision": second_precision,
                "timezone": tz_name,
                "tzinfo": tzinfo,
                "tick_fn": tick_path,
                "max_ticks": raw.get("max_ticks", self.max_ticks if have_legacy else None),
                "catchup": raw.get("catchup", self.catchup if have_legacy else False),
                "max_catchup_ticks": raw.get("max_catchup_ticks", self.max_catchup_ticks),
                "tick_error_handling": tick_error_handling,
                "description": raw.get("description", self.description),
                "group_name": raw.get("group_name", self.group_name),
                "asset_tags": raw.get("asset_tags", self.asset_tags),
                "kinds": raw.get("kinds", self.kinds),
                "owners": raw.get("owners", self.owners),
                "deps": raw.get("deps", self.deps),
            })

        names = [s["asset_name"] for s in specs]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"warm_scheduled_job: duplicate asset_name(s) across jobs: {sorted(dupes)}")

        return specs

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        job_specs = self._normalize_job_specs()
        warmup_path = self.warmup_fn
        max_seconds = self.max_seconds
        expose_info_schedules = self.expose_informational_schedules
        op_name = self.op_name or (job_specs[0]["asset_name"] if len(job_specs) == 1 else "warm_scheduled_jobs_multi")

        asset_specs = []
        for js in job_specs:
            _kinds = list(js["kinds"] or []) or ["python", "scheduling"]
            all_tags = dict(js["asset_tags"] or {})
            for k in _kinds:
                all_tags[f"dagster/kind/{k}"] = ""
            # Deliberately no `dagster/isolation` tag -- isolated is the
            # default, and this component's entire value proposition depends
            # on genuine isolation (dedicated resources, not the shared
            # code-location server). Setting `disabled` here would silently
            # defeat the design.
            asset_specs.append(dg.AssetSpec(
                key=js["asset_key"],
                description=js["description"] or self.get_description(),
                owners=js["owners"] or [],
                tags=all_tags,
                group_name=js["group_name"],
                kinds=set(_kinds),
                deps=[AssetKey.from_user_string(k) for k in (js["deps"] or [])],
            ))

        @dg.multi_asset(specs=asset_specs, name=op_name)
        def _warm_scheduled_multi_asset(context: AssetExecutionContext):
            # Resolved here (materialize time), not in build_defs
            # (component-load time) -- same convention as
            # dynamic_fanout_asset's _resolve() calls. This means a bad path
            # fails at the START of a materialize (before any warmup/tick
            # work runs), but doesn't require the user's own project modules
            # to be importable just to validate/instantiate the component
            # definition itself (e.g. from a CI schema check against
            # example.yaml).
            warmup_callable = _resolve(warmup_path, "warmup_fn") if warmup_path else None
            tick_callables = {js["asset_name"]: _resolve(js["tick_fn"], f"jobs.tick_fn ({js['asset_name']})") for js in job_specs}

            context.log.info(
                f"warm_scheduled_job: {len(job_specs)} job(s) "
                f"max_seconds={max_seconds} "
                f"jobs={[js['asset_name'] for js in job_specs]}"
            )

            warm_state = None
            if warmup_callable is not None:
                warmup_start = time.time()
                warm_state = warmup_callable(context)
                context.log.info(f"warm_scheduled_job: warmup complete in {time.time() - warmup_start:.2f}s")

            start = time.time()
            deadline: Optional[float] = (start + max_seconds) if max_seconds is not None else None
            global_stop_reason = "max_seconds" if deadline is not None else "external_signal"

            states = []
            for js in job_specs:
                from croniter import croniter
                now = datetime.now(js["tzinfo"])
                cron_iter = croniter(js["schedule"], now, second_at_beginning=js["second_precision"])
                states.append({
                    "spec": js,
                    "cron_iter": cron_iter,
                    "next_tick": _next_tick(cron_iter, now),
                    "total_ticks": 0,
                    "failed_ticks": 0,
                    "max_drift_seconds": 0.0,
                    "active": True,
                })

            while True:
                active = [s for s in states if s["active"]]
                if not active:
                    # The only way a job becomes inactive today is hitting its
                    # own max_ticks (see below) -- so this is accurate for
                    # both single-job (matches the component's original label
                    # exactly) and multi-job (every job exhausted its cap).
                    global_stop_reason = "max_ticks"
                    break
                if deadline is not None and time.time() >= deadline:
                    global_stop_reason = "max_seconds"
                    break

                current = min(active, key=lambda s: s["next_tick"])
                spec = current["spec"]
                tzinfo = spec["tzinfo"]

                now = datetime.now(tzinfo)
                remaining = (current["next_tick"] - now).total_seconds()
                if deadline is not None:
                    remaining = min(remaining, deadline - time.time())

                if remaining > 0:
                    # Two-phase sleep for tighter precision: sleep most of the
                    # duration in one shot (cheap), then fine-grained short
                    # sleeps for the last fraction of a second to absorb OS
                    # scheduler jitter close to the target instant.
                    if remaining > 0.25:
                        time.sleep(remaining - 0.2)
                    while True:
                        now = datetime.now(tzinfo)
                        remaining = (current["next_tick"] - now).total_seconds()
                        if remaining <= 0 or (deadline is not None and time.time() >= deadline):
                            break
                        time.sleep(min(remaining, 0.02))

                if deadline is not None and time.time() >= deadline:
                    global_stop_reason = "max_seconds"
                    break

                fire_time = datetime.now(tzinfo)
                drift_seconds = (fire_time - current["next_tick"]).total_seconds()
                current["max_drift_seconds"] = max(current["max_drift_seconds"], abs(drift_seconds))
                scheduled_time = current["next_tick"]

                # Advance to the next tick BEFORE running tick_fn, so a slow
                # or failed tick_fn doesn't re-derive the same "next_tick"
                # from a now-stale croniter state.
                missed = []
                current["next_tick"] = _next_tick(current["cron_iter"], fire_time)
                if spec["catchup"]:
                    guard = 0
                    while current["next_tick"] <= datetime.now(tzinfo) and guard < spec["max_catchup_ticks"]:
                        missed.append(current["next_tick"])
                        current["next_tick"] = _next_tick(current["cron_iter"], current["next_tick"])
                        guard += 1
                else:
                    while current["next_tick"] <= datetime.now(tzinfo):
                        current["next_tick"] = _next_tick(current["cron_iter"], current["next_tick"])

                tick_callable = tick_callables[spec["asset_name"]]
                for this_scheduled_time in [scheduled_time, *missed]:
                    if spec["max_ticks"] is not None and current["total_ticks"] >= spec["max_ticks"]:
                        break

                    current["total_ticks"] += 1
                    tick_start = time.time()
                    tick_meta: Dict[str, Any] = {}
                    tick_failed = False
                    try:
                        result = tick_callable(context, warm_state, this_scheduled_time)
                        if isinstance(result, dict):
                            tick_meta = result
                    except Exception as e:  # noqa: BLE001
                        tick_failed = True
                        current["failed_ticks"] += 1
                        context.log.error(
                            f"warm_scheduled_job: {spec['asset_name']} tick {current['total_ticks']} "
                            f"(scheduled {this_scheduled_time}) failed: {e}"
                        )
                        if spec["tick_error_handling"] == "raise":
                            raise
                    tick_duration = time.time() - tick_start

                    metadata = {
                        "tick_index": MetadataValue.int(current["total_ticks"]),
                        "scheduled_time": MetadataValue.text(this_scheduled_time.isoformat()),
                        "fire_time": MetadataValue.text(datetime.now(tzinfo).isoformat()),
                        "drift_seconds": MetadataValue.float(round(drift_seconds, 4)),
                        "tick_duration_seconds": MetadataValue.float(round(tick_duration, 4)),
                        "status": MetadataValue.text("failed" if tick_failed else "success"),
                    }
                    for k, v in tick_meta.items():
                        if k not in metadata:
                            metadata[k] = MetadataValue.text(str(v)) if not isinstance(v, (int, float, bool)) else (
                                MetadataValue.int(v) if isinstance(v, int) and not isinstance(v, bool) else
                                MetadataValue.float(v) if isinstance(v, float) else
                                MetadataValue.bool(v)
                            )
                    context.log_event(AssetMaterialization(
                        asset_key=spec["asset_key"],
                        description=f"{spec['asset_name']} tick {current['total_ticks']} "
                                    f"(scheduled {this_scheduled_time.isoformat()})"
                                    + (" — FAILED" if tick_failed else ""),
                        metadata=metadata,
                    ))

                if spec["max_ticks"] is not None and current["total_ticks"] >= spec["max_ticks"]:
                    current["active"] = False

            elapsed = time.time() - start
            context.log.info(
                f"warm_scheduled_job exiting: stop_reason={global_stop_reason} elapsed={elapsed:.1f}s "
                f"per_job={[(s['spec']['asset_name'], s['total_ticks'], s['failed_ticks']) for s in states]}"
            )
            for s in states:
                yield dg.MaterializeResult(
                    asset_key=s["spec"]["asset_key"],
                    metadata={
                        "stop_reason": MetadataValue.text(global_stop_reason),
                        "total_ticks": MetadataValue.int(s["total_ticks"]),
                        "failed_ticks": MetadataValue.int(s["failed_ticks"]),
                        "elapsed_seconds": MetadataValue.float(round(elapsed, 2)),
                        "max_drift_seconds": MetadataValue.float(round(s["max_drift_seconds"], 4)),
                        "schedule": MetadataValue.text(s["spec"]["schedule"]),
                        "timezone": MetadataValue.text(s["spec"]["timezone"]),
                    },
                )

        extra_assets = []
        extra_jobs = []
        extra_schedules = []
        if expose_info_schedules:
            def _make_info_asset(js: Dict[str, Any], info_asset_key: AssetKey):
                # Factory function, not a loop-body def with default-arg
                # capture: @dg.asset interprets every non-`context` parameter
                # as an asset INPUT dependency (not a Python closure trick),
                # so `_name=js["asset_name"]`-style default args would be
                # misread as a real upstream asset dependency named "_name"
                # and fail at Definitions-build time. A factory function's
                # own local scope captures `js` correctly per call, with no
                # late-binding risk and no fake asset inputs.
                @dg.asset(
                    key=info_asset_key,
                    description=(
                        f"Informational only — documents {js['asset_name']}'s real cadence "
                        f"({js['schedule']!r}). Does not control or trigger it; the real "
                        f"automation runs continuously inside the always-warm "
                        f"'{js['asset_name']}' asset. Safe to leave running if this schedule "
                        f"is accidentally toggled on: it only re-materializes this marker, "
                        f"never the real automation."
                    ),
                    group_name=js["group_name"],
                )
                def _info_asset(context: AssetExecutionContext):
                    context.log.info(
                        f"This is an informational marker for {js['asset_name']!r}'s documented "
                        f"cadence ({js['schedule']!r}) — it does nothing else. The real "
                        f"automation runs inside the always-warm '{js['asset_name']}' asset; "
                        f"see its materialization history for actual tick times and results."
                    )
                return _info_asset

            for js in job_specs:
                if js["second_precision"]:
                    continue  # a real ScheduleDefinition cannot express sub-minute cadence at all
                info_asset_key = AssetKey.from_user_string(f"{js['asset_name']}_schedule_info")
                info_job_name = f"{js['asset_name']}_schedule_info_job"
                _info_asset = _make_info_asset(js, info_asset_key)

                info_job = dg.define_asset_job(name=info_job_name, selection=[info_asset_key])
                info_schedule = dg.ScheduleDefinition(
                    name=f"{js['asset_name']}_informational_schedule",
                    cron_schedule=js["schedule"],
                    execution_timezone=js["timezone"],
                    job=info_job,
                    default_status=dg.DefaultScheduleStatus.STOPPED,
                    description=(
                        f"Documents {js['asset_name']}'s real cadence ({js['schedule']!r}); "
                        f"does not control or trigger it. The real automation runs "
                        f"continuously inside the always-warm '{js['asset_name']}' asset — "
                        f"see its materialization history for real tick times. Safe to turn "
                        f"RUNNING: it only re-materializes a no-op marker asset."
                    ),
                )
                extra_assets.append(_info_asset)
                extra_jobs.append(info_job)
                extra_schedules.append(info_schedule)

        return Definitions(
            assets=[_warm_scheduled_multi_asset, *extra_assets],
            jobs=extra_jobs,
            schedules=extra_schedules,
        )
