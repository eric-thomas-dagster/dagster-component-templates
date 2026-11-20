"""TaskChainAssetComponent.

Run an ORDERED list of heterogeneous steps (shell `command`, `sql`, or `python` callable) as a
single Dagster asset -- what a job scheduler like RunMyJobs/AutoSys/Control-M calls "one job with
multiple sequential steps." Each step runs in order; the first step to fail stops the chain (its
own `success_exit_codes`/`skip_on_exit_code` can still count a nonzero shell exit as success,
same contract as `shell_command_asset`'s own fields).

Step-dispatch pattern (keyed-dict-by-type) and `steps:` field mirror
`context_engineering_pipeline`'s own `steps:`/`_FRAME_OPS`, scaled down to this component's three
step types instead of that pipeline's dataframe-transform ops.

No inline `schedule:` block -- this repo's own convention is "schedules are their own component
type, pointed at asset_keys" (see `schedules/cron_schedule`), not config embedded on the asset
itself. Pair this component with a `CronScheduleComponent` instance naming this asset's key.

`run_window` (skip cleanly, not fail, outside a configured time window) reuses
`sensors/event_automation`'s own `business_hours_only` parser verbatim (same `"HH:MM-HH:MM tz
[days]"` format, same fail-open-on-malformed-input behavior) and the clean-skip mechanism
`assets/infrastructure/conditional_asset` already proved real: a `@dg.multi_asset` with exactly
one `AssetOut(is_required=False)`, where the skip path logs and returns with no `yield` at all --
the run still succeeds, the asset simply isn't materialized that tick, and real downstream
dependents skip too (not fail). A plain single-output `@dg.asset` has no equivalent clean-skip.
"""
import importlib
import json
import os
import subprocess
import sys
import time as _time_module
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name, partition_dimensions,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields (same shape as
    shell_command_asset/python_callable_asset's own copy of this helper)."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type (flat-fields shape) or partition_dimensions (multi-axis shape), not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN run_window (inlined per component; keep copies identical — ported verbatim from
# sensors/event_automation's own business_hours_only parser/checker, same format and same
# fail-open-on-malformed-input behavior, just renamed for this field's own name)
_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_run_window(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays). Raises
    ValueError on malformed input."""
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"run_window needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
    window, tz_name = parts[0], parts[1]
    start_str, _, end_str = window.partition("-")
    sh, _, sm = start_str.partition(":")
    eh, _, em = end_str.partition(":")
    start_min = int(sh) * 60 + int(sm or 0)
    end_min = int(eh) * 60 + int(em or 0)
    weekdays: Optional[List[int]] = None
    if len(parts) >= 3:
        weekdays = []
        for day in parts[2].split(","):
            key = day.strip().lower()[:3]
            if key in _WEEKDAY_MAP:
                weekdays.append(_WEEKDAY_MAP[key])
    return start_min, end_min, tz_name, weekdays


def _is_in_run_window(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list). Fails open
    (returns True -- never blocks) on a malformed spec or any unexpected error."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_run_window(spec)
    except Exception:
        return True
    try:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name) if tz_name.upper() != "UTC" else timezone.utc
        except Exception:
            tz = timezone.utc
        local = datetime.fromtimestamp(now_ts, tz=tz)
        cur_min = local.hour * 60 + local.minute
        if weekdays is not None and local.weekday() not in weekdays:
            return False
        if start_min <= end_min:
            return start_min <= cur_min < end_min
        return cur_min >= start_min or cur_min < end_min  # overnight window, e.g. 22:00-06:00
    except Exception:
        return True
# END run_window


def _resolve_callable(callable_path: str):
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


_VALID_STEP_TYPES = ("command", "sql", "python")


class TaskChainAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an ordered list of `command`/`sql`/`python` steps as one Dagster asset -- what a job
    scheduler like RunMyJobs/AutoSys/Control-M calls "one job with multiple sequential steps."
    See module docstring."""

    asset_key: str = Field(description="Dagster asset key (e.g. 'infra/nightly_batch').")
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of step dicts, each `{id, type, ...type-specific fields}`. Steps run "
            "in order; the FIRST step to fail stops the chain (a later step never runs) and "
            "fails the whole asset. `type`:\n"
            "- `command`: `command` (required, run via `bash -c`, supports `{partition_key}`), "
            "`cwd`, `env_vars`, `success_exit_codes` (List[int], additional codes treated as "
            "success), `skip_on_exit_code` (int, treated as a no-op success).\n"
            "- `sql`: exactly one of `resource_key` (a registered resource exposing "
            ".get_connection()) or `connection_env` (env var holding a SQLAlchemy URL, connected "
            "directly -- no Dagster resource needed), unless the chain's own `dry_run` is True, "
            "which needs neither. Plus exactly one of `sql`/`sql_file`/`sql_statements`, "
            "`parameters`, `pre_sql` (same shapes as `sql_asset`). A sql step's `resource_key` is "
            "only ever declared as a required_resource_key when it's actually in play (not "
            "dry_run) -- see `sql_asset`'s own README for why that specific constraint can't be "
            "deferred further; `connection_env` or the chain's `dry_run` avoid it entirely.\n"
            "- `python`: `callable_path` (required, 'module.path:function_name'), `args`, "
            "`kwargs` (same shape as `python_callable_asset`'s static-call mode -- no Airflow "
            "execution-context injection here; use `python_callable_asset` directly for that)."
        ),
    )

    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York mon,tue,wed,thu,fri'). "
            "Outside this window, the asset SKIPS cleanly instead of running or failing -- no steps "
            "execute, and real downstream dependents skip too (not fail). Checked once per "
            "materialization, before any step runs, not per-step. A malformed spec fails open (never "
            "blocks). Unset (default): always runs."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description="If True, log every step's 'would run' summary (the fully-resolved command/sql/callable) and skip executing any of them -- no subprocess, no SQL, no callable invocation. Output metadata always includes dry_run so a materialization history shows which runs were real.",
    )

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'task_chain' if empty.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned. With a partition type set, `{partition_key}` is substituted into any `command` step's `command`/`cwd`.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is time-based.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Retries the WHOLE chain from step 1, not just the failing step.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number of "
            "slots shared across every asset/op using the same pool name). The pool's own slot "
            "count is configured at the deployment level (dagster.yaml's concurrency: pools:, or "
            "the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Kill the wait and fail the run if the WHOLE chain (every step) hasn't finished after this many seconds. Enforced via a background-thread wrapper -- a step may continue running past the deadline (Python threads can't be forcibly killed); this bounds the asset's wait, not any one step. Unset: no timeout.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), csv=("partition_values",), tags=("asset_tags",))

    @model_validator(mode="after")
    def _validate_steps(self):
        if not self.steps:
            raise ValueError("TaskChainAssetComponent: `steps` must have at least one entry.")
        seen_ids: set = set()
        for i, step in enumerate(self.steps):
            step_id = step.get("id")
            step_type = step.get("type")
            if not step_id or not isinstance(step_id, str):
                raise ValueError(f"TaskChainAssetComponent: steps[{i}] is missing a (string) `id`.")
            if step_id in seen_ids:
                raise ValueError(f"TaskChainAssetComponent: duplicate step id {step_id!r}.")
            seen_ids.add(step_id)
            if step_type not in _VALID_STEP_TYPES:
                raise ValueError(f"TaskChainAssetComponent: steps[{i!r}] (id={step_id!r}) has type={step_type!r}; must be one of {_VALID_STEP_TYPES}.")
            if step_type == "command":
                if not step.get("command"):
                    raise ValueError(f"TaskChainAssetComponent: step {step_id!r} (type=command) requires `command`.")
            elif step_type == "sql":
                has_resource_key = bool(step.get("resource_key"))
                has_connection_env = bool(step.get("connection_env"))
                if has_resource_key and has_connection_env:
                    raise ValueError(f"TaskChainAssetComponent: step {step_id!r} (type=sql) must set at most one of `resource_key` or `connection_env`.")
                if not self.dry_run and not has_resource_key and not has_connection_env:
                    raise ValueError(f"TaskChainAssetComponent: step {step_id!r} (type=sql) requires `resource_key` or `connection_env` (unless the chain's own dry_run is True).")
                sources = [s for s in (step.get("sql"), step.get("sql_file"), step.get("sql_statements")) if s]
                if len(sources) != 1:
                    raise ValueError(f"TaskChainAssetComponent: step {step_id!r} (type=sql) must set exactly one of `sql`, `sql_file`, `sql_statements`.")
            elif step_type == "python":
                if not step.get("callable_path"):
                    raise ValueError(f"TaskChainAssetComponent: step {step_id!r} (type=python) requires `callable_path`.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Task Chain Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        key = dg.AssetKey(component.asset_key.split("/"))
        kinds = set(component.kinds) if component.kinds else {"task_chain"}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        steps = component.steps
        run_window = component.run_window
        dry_run = component.dry_run
        timeout_seconds = component.timeout_seconds

        # Only declared when actually needed (a sql step using resource_key, and not dry_run --
        # which never touches a connection). Declaring it unconditionally would force Dagster to
        # validate every sql step's resource is registered at REPOSITORY-DEF resolution time (dg
        # check defs / dg dev), even for a step using connection_env instead, or a chain that's
        # only ever run with dry_run=True -- same constraint, and same fix, as SqlAssetComponent's
        # own resource_key/connection_env/dry_run fields; see that component's README for why
        # this can't be deferred further for resource_key specifically.
        sql_resource_keys = set() if dry_run else {s["resource_key"] for s in steps if s.get("type") == "sql" and s.get("resource_key")}

        partitions_def = _build_partitions_def(
            component.partition_type, component.partition_start, component.partition_values,
            component.dynamic_partition_name, component.partition_dimensions,
            cron_schedule=component.partition_cron_schedule, cron_fmt=component.partition_cron_fmt or "%Y-%m-%d",
            end_offset=component.partition_end_offset, timezone=component.partition_timezone,
        )

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        _freshness_policy_obj = (
            _freshness_policy_compat(component.freshness_max_lag_minutes, component.freshness_cron)
            if component.freshness_max_lag_minutes is not None else None
        )

        _backfill_policy_obj = None
        if partitions_def is not None and component.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and component.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(max_partitions_per_run=component.backfill_policy_max_partitions_per_run or 1)

        out = dg.AssetOut(
            key=key,
            description=component.description or f"Task chain ({len(steps)} step(s)): " + " → ".join(s["id"] for s in steps),
            is_required=False,  # required for a clean run_window skip -- see module docstring
            group_name=component.group_name,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=kinds,
            freshness_policy=_freshness_policy_obj,
        )

        @dg.multi_asset(
            name=key.to_python_identifier(),
            outs={"result": out},
            deps=deps_keys,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            pool=component.pool,
            required_resource_keys=sql_resource_keys,
        )
        def _task_chain_asset(context: dg.AssetExecutionContext):
            if run_window and not _is_in_run_window(_time_module.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return  # no yield at all -- clean skip, see module docstring

            partition_key = context.partition_key if context.has_partition_key else None

            if dry_run:
                for step in steps:
                    context.log.info(f"[dry_run] step {step['id']!r} (type={step['type']}): would run {_describe_step(step, partition_key)}")
                yield dg.Output(None, output_name="result", metadata={"dry_run": dg.MetadataValue.bool(True)})
                return

            step_results = _run_with_timeout(_run_all_steps, timeout_seconds, steps, partition_key, context)
            yield dg.Output(
                None, output_name="result",
                metadata={"dry_run": dg.MetadataValue.bool(False), "step_results": dg.MetadataValue.json(step_results)},
            )

        return dg.Definitions(assets=[_task_chain_asset])


def _describe_step(step: dict, partition_key: Optional[str]) -> str:
    if step["type"] == "command":
        return f"$ {_substitute_partition_key(step['command'], partition_key)}"
    if step["type"] == "sql":
        sql_text = step.get("sql") or (step.get("sql_statements") or [""])[0] or f"<{step.get('sql_file')}>"
        return sql_text
    return f"{step['callable_path']}(args={step.get('args') or []}, kwargs={list((step.get('kwargs') or {}))})"


def _substitute_partition_key(text: str, partition_key: Optional[str]) -> str:
    return text.replace("{partition_key}", partition_key) if partition_key else text


def _run_all_steps(steps: List[dict], partition_key: Optional[str], context: dg.AssetExecutionContext) -> List[dict]:
    """Runs every step in order; the FIRST to fail raises dg.Failure immediately (later steps
    never run). Returns the per-step result list for every step that DID run (including the
    failing one's own partial result, attached to the Failure's metadata)."""
    results: List[dict] = []
    for step in steps:
        t0 = _time_module.time()
        try:
            if step["type"] == "command":
                detail = _run_command_step(step, partition_key, context)
            elif step["type"] == "sql":
                detail = _run_sql_step(step, context)
            else:
                detail = _run_python_step(step, context)
        except _StepFailure as e:
            results.append({"id": step["id"], "type": step["type"], "status": "failed", "duration_seconds": round(_time_module.time() - t0, 3), **e.detail})
            raise dg.Failure(
                description=f"task_chain step {step['id']!r} (type={step['type']}) failed: {e}",
                metadata={"step_results": dg.MetadataValue.json(results)},
            ) from e
        results.append({"id": step["id"], "type": step["type"], "status": "success", "duration_seconds": round(_time_module.time() - t0, 3), **detail})
    return results


class _StepFailure(Exception):
    def __init__(self, message: str, detail: Optional[dict] = None):
        super().__init__(message)
        self.detail = detail or {}


def _run_command_step(step: dict, partition_key: Optional[str], context: dg.AssetExecutionContext) -> dict:
    cmd = _substitute_partition_key(step["command"], partition_key)
    cwd = _substitute_partition_key(step["cwd"], partition_key) if step.get("cwd") else None
    merged_env = {**os.environ, **(step.get("env_vars") or {})}
    context.log.info(f"[{step['id']}] $ {cmd}")
    proc = subprocess.Popen(
        cmd, shell=True, cwd=cwd, env=merged_env, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    stdout_lines = []
    for line in proc.stdout or []:
        sys.stdout.write(line)
        stdout_lines.append(line)
    stderr_data = (proc.stderr.read() if proc.stderr else "") or ""
    proc.wait()

    skip_on_exit_code = step.get("skip_on_exit_code")
    success_exit_codes = step.get("success_exit_codes") or []
    detail = {"exit_code": proc.returncode}
    if skip_on_exit_code is not None and proc.returncode == skip_on_exit_code:
        detail["skipped"] = True
        return detail
    if proc.returncode != 0 and proc.returncode not in success_exit_codes:
        raise _StepFailure(f"exit code {proc.returncode}", {"exit_code": proc.returncode, "stderr": stderr_data[:2000]})
    return detail


def _run_sql_step(step: dict, context: dg.AssetExecutionContext) -> dict:
    if step.get("connection_env"):
        from sqlalchemy import create_engine

        connection_env = step["connection_env"]
        url = dg.EnvVar(connection_env).get_value()
        if not url:
            raise _StepFailure(f"connection_env={connection_env!r} is set but that environment variable is empty/unset")
        acquire = nullcontext(create_engine(url).raw_connection())
    else:
        resource_key = step["resource_key"]
        resource = getattr(context.resources, resource_key)
        if not hasattr(resource, "get_connection"):
            raise _StepFailure(f"resource {resource_key!r} must expose .get_connection()")
        raw_or_cm = resource.get_connection()
        acquire = raw_or_cm if hasattr(raw_or_cm, "__enter__") else nullcontext(raw_or_cm)
    statement_count = 0
    try:
        with acquire as conn:
            cursor = conn.cursor()
            try:
                for stmt in (step.get("pre_sql") or []):
                    context.log.info(stmt)
                    cursor.execute(stmt)
                parameters = step.get("parameters")
                if step.get("sql_statements"):
                    for stmt in step["sql_statements"]:
                        context.log.info(stmt)
                        cursor.execute(stmt, parameters) if parameters else cursor.execute(stmt)
                        statement_count += 1
                else:
                    sql_text = step["sql"] if step.get("sql") else open(step["sql_file"]).read()
                    context.log.info(sql_text)
                    cursor.execute(sql_text, parameters) if parameters else cursor.execute(sql_text)
                    statement_count += 1
                try:
                    conn.commit()
                except Exception as e:  # not every dialect supports/needs commit
                    context.log.debug(f"conn.commit() raised (tolerated): {e}")
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
    except _StepFailure:
        raise
    except Exception as e:
        raise _StepFailure(str(e)) from e
    return {"statement_count": statement_count}


def _run_python_step(step: dict, context: dg.AssetExecutionContext) -> dict:
    fn = _resolve_callable(step["callable_path"])
    args = step.get("args") or []
    kwargs = step.get("kwargs") or {}
    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        raise _StepFailure(str(e)) from e
    context.log.info(f"[{step['id']}] {step['callable_path']} returned: {result!r}"[:500])
    return {"result_repr": repr(result)[:500]}
