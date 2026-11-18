"""PythonSensorComponent.

Wraps an arbitrary Python callable in a Dagster sensor -- mirrors Airflow's `PythonSensor`
(poke a callable, run when it returns truthy) and, in `discover_partitions` mode, the "list
items via a callable" dynamic-task-mapping source (the one discovery source
`s3_monitor`/`gcs_monitor`/`adls_monitor`/`filesystem_monitor`/`sql_monitor` don't already
cover -- those are prefix/table-specific; this is for anything else: an API call, a config file,
a custom registry).

Two modes, not two components, since they're the same underlying shape (poll a callable, decide
what to run) differing only in cardinality:
  - `trigger`: callable returns one bool (+ optional payload) -> at most one RunRequest per tick.
  - `discover_partitions`: callable returns a list of items -> one dynamic partition + RunRequest
    per NEW item, reusing the exact AddDynamicPartitionsRequest/pruning mechanism already proven
    in s3_monitor/sql_monitor.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN dynamic-partition pruning (inlined per component; keep copies identical — see tests/test_dynamic_partition_pruning.py)
def _prune_stale_dynamic_partitions(state_path, keep_days, now, newly_added_keys):
    """Tracks {partition_key: first_seen_iso} in a local JSON file (independent of the
    sensor's own cursor) and returns (stale_keys, updated_state) -- the caller issues a
    DeleteDynamicPartitionsRequest for stale_keys and persists updated_state. Dynamic
    partitions otherwise accumulate forever. Returns ([], {}) immediately (no file I/O) when
    keep_days is falsy -- pruning is opt-in."""
    if not keep_days:
        return [], {}
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path) as f:
                state = json.load(f)
        except Exception:  # noqa: BLE001
            state = {}
    for k in newly_added_keys:
        state.setdefault(k, now.isoformat())
    cutoff = now - timedelta(days=keep_days)
    stale = [k for k, seen in state.items() if datetime.fromisoformat(seen) < cutoff]
    for k in stale:
        state.pop(k, None)
    return stale, state


def _write_prune_state(state_path, state):
    try:
        with open(state_path, "w") as f:
            json.dump(state, f)
    except Exception:  # noqa: BLE001
        pass
# END dynamic-partition pruning


def _load_known_keys(state_path: str) -> set:
    """Separate from the pruning block above (which merges + ages out but doesn't report
    which keys were already known): discover_partitions mode needs to know which of the
    callable's returned items are genuinely NEW before issuing a RunRequest for them, since
    (unlike a watermark/last-modified cursor) a callable may return its full current item set
    every tick, not just new arrivals."""
    if not os.path.exists(state_path):
        return set()
    try:
        with open(state_path) as f:
            return set(json.load(f).keys())
    except Exception:  # noqa: BLE001
        return set()


def _resolve_callable(callable_path: str):
    import importlib
    if ":" not in callable_path:
        raise ValueError(f"callable_path {callable_path!r} must be 'module.path:function_name'")
    mod_path, fn_name = callable_path.rsplit(":", 1)
    fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
    if not callable(fn):
        raise ValueError(f"callable_path {callable_path!r}: {fn_name!r} is not callable")
    return fn


class PythonSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wraps an arbitrary Python callable in a Dagster sensor -- mirrors Airflow's
    `PythonSensor` (trigger mode) and serves as the generic "discover items via a callable"
    dynamic-task-mapping source (discover_partitions mode)."""

    sensor_name: str = Field(description="Unique sensor name.")
    callable_path: str = Field(description="'module.path:function_name'. Called as fn(context, *args, **kwargs) every tick.")
    job_name: str = Field(description="Name of the job to trigger.")
    mode: str = Field(
        default="trigger",
        description=(
            "'trigger' (default, mirrors Airflow's PythonSensor): the callable returns a "
            "bool (fire or skip), a non-empty dict (fire, used as run_config), or a "
            "(bool, dict) 2-tuple (fire if the bool is truthy, with that run_config). "
            "'discover_partitions': the callable returns a list of item keys (each a bare "
            "str, or a (key, payload_dict) 2-tuple) -- each NEW key becomes a dynamic "
            "partition + one RunRequest, mirroring s3_monitor/sql_monitor's own mechanism."
        ),
    )
    fire_mode: str = Field(
        default="every_true_tick",
        description=(
            "Only used in mode='trigger'. 'every_true_tick' (default): fire a new run every "
            "tick the callable returns truthy. 'edge_rising': fire only on the transition "
            "from falsy to truthy (tracked in the sensor's own cursor) -- avoids re-firing "
            "every tick while a condition stays true. 'once': fire exactly one time ever "
            "(cursor-gated), then never again, regardless of how many more times the "
            "callable returns truthy -- closest to Airflow's PythonSensor, whose task "
            "instance is terminal once it succeeds."
        ),
    )
    args: Optional[List[Any]] = Field(default=None, description="Static positional args passed to the callable after context.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword args passed to the callable.")

    dynamic_partitions_name: Optional[str] = Field(
        default=None,
        description="Required when mode='discover_partitions'. Must match the downstream asset's `dynamic_partition_name:` field.",
    )
    dynamic_partition_keep_days: Optional[int] = Field(
        default=None,
        description="Only used in mode='discover_partitions'. When set, partitions older than this many days are deleted. Unset: no pruning (the dedup state file is still kept regardless, to avoid re-firing on already-seen items).",
    )
    dynamic_partition_state_path: Optional[str] = Field(
        default=None,
        description="Only used in mode='discover_partitions'. Where {partition_key: first_seen} state (dedup + optional pruning) is persisted. Defaults to './{sensor_name}_dynamic_partition_state.json'.",
    )

    op_name: Optional[str] = Field(default="config", description="Op name to key run_config under, as `{'ops': {op_name: {'config': {...}}}}`.")
    minimum_interval_seconds: int = Field(default=30, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="stopped", description="'running' or 'stopped'.")

    @model_validator(mode="after")
    def _validate_mode(self):
        if self.mode not in ("trigger", "discover_partitions"):
            raise ValueError(f"mode must be 'trigger' or 'discover_partitions', got {self.mode!r}.")
        if self.mode == "discover_partitions" and not self.dynamic_partitions_name:
            raise ValueError("mode='discover_partitions' requires dynamic_partitions_name.")
        if self.fire_mode not in ("every_true_tick", "edge_rising", "once"):
            raise ValueError(f"fire_mode must be 'every_true_tick' | 'edge_rising' | 'once', got {self.fire_mode!r}.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Python Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sensor_name = self.sensor_name
        callable_path = self.callable_path
        job_name = self.job_name
        mode = self.mode
        fire_mode = self.fire_mode
        static_args = list(self.args or [])
        static_kwargs = dict(self.kwargs or {})
        dynamic_partitions_name = self.dynamic_partitions_name
        dynamic_partition_keep_days = self.dynamic_partition_keep_days
        dynamic_partition_state_path = self.dynamic_partition_state_path or f"./{sensor_name}_dynamic_partition_state.json"
        op_name = self.op_name or "config"
        minimum_interval_seconds = self.minimum_interval_seconds

        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status.lower() == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
        )
        def _python_sensor(context: dg.SensorEvaluationContext):
            fn = _resolve_callable(callable_path)
            try:
                result = fn(context, *static_args, **static_kwargs)
            except Exception as e:  # noqa: BLE001
                context.log.error(f"callable {callable_path!r} raised: {e}")
                return dg.SkipReason(f"callable raised {type(e).__name__}: {e}")

            if mode == "discover_partitions":
                return _handle_discover_partitions(
                    context, result or [], dynamic_partitions_name,
                    dynamic_partition_keep_days, dynamic_partition_state_path, op_name,
                )
            return _handle_trigger(context, result, fire_mode, op_name)

        return dg.Definitions(sensors=[_python_sensor])


def _handle_trigger(context: "dg.SensorEvaluationContext", result: Any, fire_mode: str, op_name: str):
    """mode='trigger': result is a bool, a non-empty dict (run_config), or a (bool, dict)
    2-tuple. Decides whether/how to fire based on fire_mode."""
    if isinstance(result, tuple) and len(result) == 2:
        truthy, payload = result
    elif isinstance(result, dict):
        truthy, payload = bool(result), result
    else:
        truthy, payload = bool(result), None

    if fire_mode == "once":
        if context.cursor == "fired":
            return dg.SkipReason("fire_mode='once': already fired its single run; never fires again.")
        if not truthy:
            return dg.SkipReason("callable returned falsy; not yet ready to fire.")
        run_key = f"fired-{int(time.time() * 1000)}"
        return dg.SensorResult(
            run_requests=[_build_run_request(run_key, payload, op_name)],
            cursor="fired",
        )

    if fire_mode == "edge_rising":
        was_true = context.cursor == "true"
        if not truthy:
            return dg.SensorResult(cursor="false") if was_true else dg.SkipReason("callable returned falsy.")
        if was_true:
            return dg.SkipReason("callable still truthy (no rising edge); already fired on the transition.")
        run_key = f"edge-{int(time.time() * 1000)}"
        return dg.SensorResult(
            run_requests=[_build_run_request(run_key, payload, op_name)],
            cursor="true",
        )

    # every_true_tick (default)
    if not truthy:
        return dg.SkipReason("callable returned falsy; nothing to run this tick.")
    run_key = f"tick-{int(time.time() * 1000)}"
    return dg.SensorResult(run_requests=[_build_run_request(run_key, payload, op_name)])


def _build_run_request(run_key: str, payload: Optional[dict], op_name: str) -> "dg.RunRequest":
    run_config = {"ops": {op_name: {"config": payload}}} if payload else None
    return dg.RunRequest(run_key=run_key, run_config=run_config)


def _handle_discover_partitions(
    context: "dg.SensorEvaluationContext",
    items: List[Union[str, Tuple[str, dict]]],
    dynamic_partitions_name: str,
    dynamic_partition_keep_days: Optional[int],
    dynamic_partition_state_path: str,
    op_name: str,
):
    """mode='discover_partitions': items is the callable's CURRENT full set of item keys
    (not just new ones) -- diffed against the dedup state file to find what's actually new,
    since (unlike a watermark cursor) there's no scalar high-water mark for an arbitrary list."""
    parsed: List[Tuple[str, Optional[dict]]] = []
    for item in items:
        if isinstance(item, (tuple, list)) and len(item) == 2:
            parsed.append((str(item[0]), item[1]))
        else:
            parsed.append((str(item), None))

    known = _load_known_keys(dynamic_partition_state_path)
    new_items = [(k, p) for k, p in parsed if k not in known]
    all_keys = [k for k, _ in parsed]

    run_requests = [
        dg.RunRequest(
            run_key=f"{dynamic_partitions_name}-{key}",
            partition_key=key,
            run_config=({"ops": {op_name: {"config": payload}}} if payload else None),
        )
        for key, payload in new_items
    ]

    dynamic_partitions_requests = []
    if new_items:
        dynamic_partitions_requests.append(
            dg.AddDynamicPartitionsRequest(
                partitions_def_name=dynamic_partitions_name,
                partition_keys=[k for k, _ in new_items],
            )
        )

    # Pruning runs every tick (not just when new items are found) -- stale partitions
    # must age out even on a quiet tick. Also keeps the dedup state file in sync with
    # every key the callable currently reports, not just brand-new ones.
    stale_keys, pruned_state = _prune_stale_dynamic_partitions(
        dynamic_partition_state_path, dynamic_partition_keep_days, datetime.now(timezone.utc), all_keys,
    )
    if stale_keys:
        dynamic_partitions_requests.append(
            dg.DeleteDynamicPartitionsRequest(partitions_def_name=dynamic_partitions_name, partition_keys=stale_keys)
        )
    if dynamic_partition_keep_days:
        _write_prune_state(dynamic_partition_state_path, pruned_state)
    else:
        # No pruning configured, but the dedup state file is still mandatory --
        # write it directly (the shared helper above returns {} without touching
        # the file at all when keep_days is falsy).
        merged = _read_state(dynamic_partition_state_path)
        now_iso = datetime.now(timezone.utc).isoformat()
        for k in all_keys:
            merged.setdefault(k, now_iso)
        _write_prune_state(dynamic_partition_state_path, merged)

    if run_requests or dynamic_partitions_requests:
        context.log.info(f"discover_partitions: {len(new_items)} new item(s), {len(stale_keys)} pruned")
        return dg.SensorResult(run_requests=run_requests, dynamic_partitions_requests=dynamic_partitions_requests)
    return dg.SkipReason("discover_partitions: no new items.")


def _read_state(state_path: str) -> dict:
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path) as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}
