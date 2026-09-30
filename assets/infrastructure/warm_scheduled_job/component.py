"""WarmScheduledJobComponent — an isolated, always-warm process that manages
its own cron schedule internally, bypassing Dagster's SchedulerDaemon entirely.

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
  2. Parses `schedule` (an ordinary cron string) with `croniter` and manages
     its OWN tick timing — computing the exact next-fire instant and
     sleeping precisely until it, rather than asking any Dagster daemon to
     evaluate the cron on a polling interval.
  3. Calls `tick_fn(context, warm_state, scheduled_time)` directly as an
     in-process Python function call at each tick — never through Dagster's
     run-launching APIs (no `RunRequest`, no `dg api run launch`, no
     `materialize()` against the instance). That's what makes each tick
     truly instant: it's a function call in an already-warm process, not a
     new run.
  4. Emits an `AssetMaterialization` per tick (with measured drift against
     the scheduled instant, so the precision this buys you is directly
     observable in the catalog, not just asserted) for lineage/observability
     — this is the one place Dagster's own machinery is still used, and it's
     optional bookkeeping, not the trigger mechanism.
  5. Exits cleanly at `max_seconds` (same bounded-run pattern as
     `StreamingConsumerComponent`) so a paired health sensor can relaunch —
     re-paying the warmup cost once per `max_seconds` window, not once per
     tick. If your schedule fires every 15 minutes and warmup takes 45s,
     `max_seconds: 3600` pays that 45s once an hour, not 4x an hour.

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

## What this does NOT give you

- No Dagster Schedules-tab entry, no visible "next tick" in the UI — there
  is no real `ScheduleDefinition` here on purpose. If catalog visibility of
  the cron matters, the `schedule` field's value is plain component config
  you could separately surface in a dashboard/description; don't wire it to
  an actual (non-functional) `ScheduleDefinition` just for display, since a
  schedule object that never fires reads as broken in the Dagster+ UI.
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


class WarmScheduledJobComponent(Component, Model, Resolvable):
    """An isolated, always-warm job that manages its own cron schedule
    in-process instead of relying on Dagster's minute-granular
    SchedulerDaemon. See the module docstring for the full rationale.

    Example:
        ```yaml
        type: dagster_community_components.WarmScheduledJobComponent
        attributes:
          asset_name: pricing_refresh_warm
          schedule: "*/15 * * * *"
          warmup_fn: "myproject.jobs.pricing:warmup"
          tick_fn: "myproject.jobs.pricing:run_tick"
          max_seconds: 3600
        ```
    """

    asset_name: str = Field(description="Output asset name (also used by the paired health sensor's asset_selection).")

    schedule: str = Field(
        description=(
            "A cron string. Standard 5-field (e.g. '*/15 * * * *' for every "
            "15 minutes, '0 9 * * *' for daily at 9am) by default. Evaluated "
            "by this component's own internal loop via `croniter` — this is "
            "NOT a Dagster ScheduleDefinition and is never handed to the "
            "SchedulerDaemon, which is the whole point: the daemon's tick "
            "evaluation is minute-granular by architecture, not by load. Set "
            "`second_precision: true` to use 6-field cron (seconds as the "
            "leading field, e.g. '*/30 * * * * *' for every 30 seconds) — a "
            "schedule finer than Dagster's native scheduler can express at "
            "all, not just one it can express but not hit precisely."
        ),
    )
    second_precision: bool = Field(
        default=False,
        description="Parse `schedule` as 6-field cron (seconds-leading) instead of standard 5-field.",
    )
    timezone: str = Field(
        default="UTC",
        description="IANA timezone name (e.g. 'America/New_York') the cron string is evaluated in.",
    )

    warmup_fn: Optional[str] = Field(
        default=None,
        description=(
            "'module.path:function_name', called ONCE per bounded run "
            "lifetime (not per tick) as `warmup_fn(context) -> Any`. Do the "
            "expensive setup here (load a model, open connections) — the "
            "return value is passed as `warm_state` to every `tick_fn` call "
            "in this run. Optional: omit if there's nothing to warm up."
        ),
    )
    tick_fn: str = Field(
        description=(
            "'module.path:function_name', called once per precise scheduled "
            "tick as `tick_fn(context, warm_state, scheduled_time) -> "
            "Optional[dict]`, as a direct in-process function call — never "
            "through Dagster's run-launching APIs, which is what makes each "
            "tick instant. An optional returned dict is merged into that "
            "tick's AssetMaterialization metadata."
        ),
    )

    max_seconds: Optional[int] = Field(
        default=3600,
        description=(
            "Bounded run duration in seconds, same pattern as "
            "StreamingConsumerComponent. Set LESS than your Dagster+ "
            "Serverless per-run timeout. `null` for a truly unbounded loop "
            "(runs until the platform kills the process) — bounded is safer: "
            "graceful exit + summary metadata, then the paired health sensor "
            "launches the next run (re-paying warmup once per window, not "
            "once per tick)."
        ),
    )
    max_ticks: Optional[int] = Field(
        default=None,
        description="Optional cap on total ticks executed before exit (in addition to max_seconds).",
    )
    catchup: bool = Field(
        default=False,
        description=(
            "If a tick's scheduled instant has already passed by the time "
            "the loop checks again (e.g. slow process start, or the "
            "previous tick's tick_fn overran into this one), default "
            "(false) skips straight to the next FUTURE tick. Set true to "
            "execute every missed tick back-to-back instead (bounded by "
            "max_catchup_ticks to avoid a pileup)."
        ),
    )
    max_catchup_ticks: int = Field(
        default=10,
        description="Safety cap on consecutive missed ticks executed back-to-back when catchup=true.",
    )
    tick_error_handling: str = Field(
        default="continue",
        description=(
            "'continue' (default): a tick_fn exception is logged + reported "
            "as a failed-tick materialization, and the loop continues to the "
            "next scheduled tick — a single tick's transient failure doesn't "
            "force a full restart (and re-paid warmup cost). 'raise': the "
            "exception propagates and fails the whole run immediately."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @classmethod
    def get_description(cls) -> str:
        return "Isolated, always-warm job that manages its own precise cron schedule in-process."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        schedule_str = self.schedule
        tz_name = self.timezone
        warmup_path = self.warmup_fn
        tick_path = self.tick_fn
        max_seconds = self.max_seconds
        max_ticks = self.max_ticks
        catchup = self.catchup
        max_catchup_ticks = self.max_catchup_ticks
        tick_error_handling = self.tick_error_handling

        if tick_error_handling not in ("continue", "raise"):
            raise ValueError(
                f"warm_scheduled_job: tick_error_handling must be 'continue' or 'raise', got {tick_error_handling!r}."
            )

        try:
            from croniter import croniter
        except ImportError:
            raise ImportError("warm_scheduled_job requires croniter: pip install croniter")
        try:
            from zoneinfo import ZoneInfo
            tzinfo = ZoneInfo(tz_name)
        except Exception as e:
            raise ValueError(f"warm_scheduled_job: invalid timezone {tz_name!r}: {e}") from e

        second_precision = self.second_precision
        # croniter.is_valid() is too lenient to be the sole gate here --
        # confirmed live that it accepts 6-field strings even when
        # second_at_beginning=False, and even accepts a malformed 7-field
        # string as "valid". Enforce the exact expected field count
        # ourselves first, so a stray extra field or a forgotten
        # second_precision: true gives a clear error instead of silently
        # misinterpreting which field means what.
        expected_fields = 6 if second_precision else 5
        actual_fields = len(schedule_str.split())
        if actual_fields != expected_fields:
            raise ValueError(
                f"warm_scheduled_job: schedule={schedule_str!r} has {actual_fields} fields, "
                f"expected exactly {expected_fields} ({'6-field second-precision' if second_precision else '5-field standard'} cron). "
                f"{'Set second_precision: true for a seconds-leading field.' if actual_fields == 6 and not second_precision else ''}"
            )
        if not croniter.is_valid(schedule_str, second_at_beginning=second_precision):
            raise ValueError(
                f"warm_scheduled_job: schedule={schedule_str!r} is not a valid "
                f"{'6-field second-precision' if second_precision else '5-field'} cron string."
            )

        _kinds = list(self.kinds or []) or ["python", "scheduling"]
        all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            all_tags[f"dagster/kind/{k}"] = ""
        # Deliberately no `dagster/isolation` tag -- isolated is the default,
        # and this component's entire value proposition depends on genuine
        # isolation (dedicated resources, not the shared code-location
        # server). Setting `disabled` here would silently defeat the design.

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            kinds=set(_kinds),
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _warm_scheduled_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            # Resolved here (materialize time), not in build_defs (component-
            # load time) -- same convention as dynamic_fanout_asset's
            # _resolve() calls. This means a bad path fails at the START of
            # a materialize (before any warmup/tick work runs), but doesn't
            # require the user's own project modules to be importable just
            # to validate/instantiate the component definition itself (e.g.
            # from a CI schema check against example.yaml).
            warmup_callable = _resolve(warmup_path, "warmup_fn") if warmup_path else None
            tick_callable = _resolve(tick_path, "tick_fn")

            context.log.info(
                f"warm_scheduled_job: schedule={schedule_str!r} tz={tz_name!r} "
                f"max_seconds={max_seconds} max_ticks={max_ticks}"
            )

            warm_state = None
            if warmup_callable is not None:
                warmup_start = time.time()
                warm_state = warmup_callable(context)
                context.log.info(f"warm_scheduled_job: warmup complete in {time.time() - warmup_start:.2f}s")

            start = time.time()
            deadline: Optional[float] = (start + max_seconds) if max_seconds is not None else None
            stop_reason = "max_seconds" if deadline is not None else "external_signal"

            now = datetime.now(tzinfo)
            cron_iter = croniter(schedule_str, now, second_at_beginning=second_precision)
            next_tick = _next_tick(cron_iter, now)

            total_ticks = 0
            failed_ticks = 0
            max_drift_seconds = 0.0

            while deadline is None or time.time() < deadline:
                if max_ticks is not None and total_ticks >= max_ticks:
                    stop_reason = "max_ticks"
                    break

                now = datetime.now(tzinfo)
                remaining = (next_tick - now).total_seconds()

                if remaining > 0:
                    # Two-phase sleep for tighter precision: sleep most of the
                    # duration in one shot (cheap), then fine-grained short
                    # sleeps for the last fraction of a second to absorb OS
                    # scheduler jitter close to the target instant.
                    if remaining > 0.25:
                        time.sleep(remaining - 0.2)
                    while True:
                        now = datetime.now(tzinfo)
                        remaining = (next_tick - now).total_seconds()
                        if remaining <= 0:
                            break
                        time.sleep(min(remaining, 0.02))

                fire_time = datetime.now(tzinfo)
                drift_seconds = (fire_time - next_tick).total_seconds()
                max_drift_seconds = max(max_drift_seconds, abs(drift_seconds))
                scheduled_time = next_tick

                # Advance to the next tick BEFORE running tick_fn, so a slow
                # or failed tick_fn doesn't re-derive the same "next_tick"
                # from a now-stale croniter state.
                missed = []
                next_tick = _next_tick(cron_iter, fire_time)
                if catchup:
                    # Collect any additional ticks that are ALSO already in
                    # the past (we were behind by more than one tick), up to
                    # max_catchup_ticks, so we execute each one instead of
                    # skipping straight to the next future tick.
                    guard = 0
                    while next_tick <= datetime.now(tzinfo) and guard < max_catchup_ticks:
                        missed.append(next_tick)
                        next_tick = _next_tick(cron_iter, next_tick)
                        guard += 1
                else:
                    # Default: if we're behind, skip straight to the next
                    # FUTURE tick rather than piling up catch-up executions.
                    while next_tick <= datetime.now(tzinfo):
                        next_tick = _next_tick(cron_iter, next_tick)

                for this_scheduled_time in [scheduled_time, *missed]:
                    total_ticks += 1
                    tick_start = time.time()
                    tick_meta: Dict[str, Any] = {}
                    tick_failed = False
                    try:
                        result = tick_callable(context, warm_state, this_scheduled_time)
                        if isinstance(result, dict):
                            tick_meta = result
                    except Exception as e:  # noqa: BLE001
                        tick_failed = True
                        failed_ticks += 1
                        context.log.error(f"warm_scheduled_job: tick {total_ticks} (scheduled {this_scheduled_time}) failed: {e}")
                        if tick_error_handling == "raise":
                            raise
                    tick_duration = time.time() - tick_start

                    metadata = {
                        "tick_index": MetadataValue.int(total_ticks),
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
                        asset_key=asset_name,
                        description=f"tick {total_ticks} (scheduled {this_scheduled_time.isoformat()})"
                                    + (" — FAILED" if tick_failed else ""),
                        metadata=metadata,
                    ))

            elapsed = time.time() - start
            context.log.info(
                f"warm_scheduled_job exiting: stop_reason={stop_reason} "
                f"ticks={total_ticks} failed={failed_ticks} elapsed={elapsed:.1f}s "
                f"max_drift_seconds={max_drift_seconds:.4f}"
            )
            context.add_output_metadata({
                "stop_reason": MetadataValue.text(stop_reason),
                "total_ticks": MetadataValue.int(total_ticks),
                "failed_ticks": MetadataValue.int(failed_ticks),
                "elapsed_seconds": MetadataValue.float(round(elapsed, 2)),
                "max_drift_seconds": MetadataValue.float(round(max_drift_seconds, 4)),
                "schedule": MetadataValue.text(schedule_str),
                "timezone": MetadataValue.text(tz_name),
            })
            return {
                "stop_reason": stop_reason,
                "total_ticks": total_ticks,
                "failed_ticks": failed_ticks,
                "elapsed_seconds": elapsed,
                "max_drift_seconds": max_drift_seconds,
            }

        return Definitions(assets=[_warm_scheduled_asset])
