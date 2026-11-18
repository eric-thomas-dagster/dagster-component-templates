"""PartitionWindowSensorComponent.

Waits for a WINDOW of an upstream asset's partitions, then triggers a downstream job --
Airflow 3.3's `RollupMapper`/`FanOutMapper` + `DayWindow`/`WeekWindow`/`MonthWindow`/
`SegmentWindow` + `WaitForAll`/`MinimumCount`, verified against Airflow's own official
`example_asset_partition` example DAG source (airflow.sdk), not guessed.

Why this needs a sensor and isn't just a Dagster `PartitionMapping`: Dagster's own
`AutomationCondition.all_deps_match(...)` is documented to be true "if AT LEAST ONE
partition of... the target's dependencies" matches -- confirmed live and via its own
docstring -- not "ALL mapped partitions of this one dependency." There is no built-in
Dagster primitive for "require ALL (or N of M) partitions within one partition-mapped
dependency before firing" -- the exact thing Airflow's RollupMapper/WaitForAll/
MinimumCount model. This sensor checks `instance.get_materialized_partitions()` directly
instead of relying on that automation-condition vocabulary.

Two modes, mirroring Airflow's two mapper families:
  - `mode="rollup"` (RollupMapper, N upstream partitions -> 1 downstream run): waits until
    a window of upstream keys is (fully, or partially per `wait_policy`) materialized, then
    fires ONE RunRequest for the rolled-up downstream key. Fires once per window (tracked
    in the sensor's own cursor) -- matches `daily_team_a_rollup` (hourly->daily, WaitForAll),
    `monthly_team_a_rollup` (daily->monthly), `segment_region_stats_rollup`/
    `segment_region_stats_early_rollup` (categorical, WaitForAll / MinimumCount(2)).
  - `mode="fan_out"` (FanOutMapper, 1 upstream partition -> N downstream runs): on each
    NEWLY materialized upstream partition, fires one RunRequest per downstream key in its
    window -- matches `daily_inference`/`trailing_week_inference` (weekly->7 daily runs,
    `direction` forward/backward) and `scatter_live_region_to_segments` (1 region ->
    1 same-named downstream run, the window_type='segment' degenerate case).

`window_type='segment'` is the categorical case (`segment_keys`, not time-based -- Airflow's
`SegmentWindow`). `window_type` in ('day', 'week', 'month') is the time-based case (Airflow's
`DayWindow`/`WeekWindow`/`MonthWindow`): upstream keys are parsed via `upstream_key_format`
and truncated to a day/week(Monday-start)/month anchor, formatted via `downstream_key_format`
for the rolled-up (or fanned-out) key(s).
"""
import calendar
import json
from datetime import datetime, timedelta
from typing import Any, List, Literal, Optional, Set, Tuple

import dagster as dg
from pydantic import Field, model_validator


def _parse_upstream_key(key: str, fmt: str) -> Optional[datetime]:
    try:
        return datetime.strptime(key, fmt)
    except ValueError:
        return None


def _rollup_anchor(dt: datetime, window_type: str) -> datetime:
    if window_type == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if window_type == "week":
        start = dt - timedelta(days=dt.weekday())  # Monday-start, matching Airflow's WeekWindow
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    if window_type == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unknown time-based window_type: {window_type!r}")


def _expected_upstream_keys(anchor: datetime, window_type: str, upstream_key_format: str) -> Set[str]:
    if window_type == "day":
        return {(anchor + timedelta(hours=h)).strftime(upstream_key_format) for h in range(24)}
    if window_type == "week":
        return {(anchor + timedelta(days=d)).strftime(upstream_key_format) for d in range(7)}
    if window_type == "month":
        days_in_month = calendar.monthrange(anchor.year, anchor.month)[1]
        return {(anchor + timedelta(days=d)).strftime(upstream_key_format) for d in range(days_in_month)}
    raise ValueError(f"unknown time-based window_type: {window_type!r}")


def _fan_out_keys(anchor: datetime, window_type: str, count: int, direction: str, downstream_key_format: str) -> List[str]:
    step = timedelta(hours=1) if window_type == "day" else timedelta(days=1)
    if direction == "forward":
        return [(anchor + i * step).strftime(downstream_key_format) for i in range(count)]
    return [(anchor - i * step).strftime(downstream_key_format) for i in range(count)][::-1]


def _load_cursor(context: "dg.SensorEvaluationContext") -> dict:
    if not context.cursor:
        return {}
    try:
        return json.loads(context.cursor)
    except (TypeError, ValueError):
        return {}


class PartitionWindowSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Waits for a window of an upstream asset's partitions (categorical or time-based),
    then triggers a downstream job -- Airflow 3.3's RollupMapper/FanOutMapper +
    DayWindow/WeekWindow/MonthWindow/SegmentWindow + WaitForAll/MinimumCount."""

    sensor_name: str = Field(description="Unique sensor name.")
    upstream_asset_key: str = Field(description="Asset key (slash-delimited) being watched for partition materializations.")
    job_name: str = Field(description="Name of the downstream job to trigger.")
    mode: Literal["rollup", "fan_out"] = Field(
        description="'rollup' (Airflow RollupMapper, N upstream partitions -> 1 downstream run). 'fan_out' (Airflow FanOutMapper, 1 upstream partition -> N downstream runs).",
    )
    window_type: Literal["segment", "day", "week", "month"] = Field(
        description="'segment': an explicit categorical key list (Airflow SegmentWindow). 'day'/'week'/'month': a time-based window (Airflow Day/Week/MonthWindow) -- upstream keys parsed via upstream_key_format, truncated to a day/week(Monday-start)/month anchor.",
    )
    segment_keys: Optional[List[str]] = Field(default=None, description="window_type='segment' only (required then): the explicit, fixed set of upstream partition keys that make up one segment (e.g. ['us', 'eu', 'apac']).")
    upstream_key_format: str = Field(default="%Y-%m-%d-%H:%M", description="window_type in ('day','week','month') only: strptime format of the upstream asset's own partition keys.")
    downstream_key_format: str = Field(default="%Y-%m-%d", description="window_type in ('day','week','month') only: strftime format for the rolled-up (mode='rollup') or fanned-out (mode='fan_out') downstream key(s).")

    wait_policy: Literal["all", "minimum_count"] = Field(default="all", description="mode='rollup' only: 'all' (Airflow WaitForAll, the default) requires every expected key in the window; 'minimum_count' (Airflow MinimumCount(n)) fires once `minimum_count` of them have arrived.")
    minimum_count: Optional[int] = Field(default=None, description="mode='rollup', wait_policy='minimum_count' only (required then).")

    max_downstream_keys: Optional[int] = Field(default=None, description="mode='fan_out', window_type in ('day','week','month') only (required then): how many downstream keys to fan out to (Airflow's max_downstream_keys, e.g. 7 for a week of daily runs).")
    direction: Literal["forward", "backward"] = Field(default="forward", description="mode='fan_out', window_type in ('day','week','month') only: 'forward' (Airflow Window.Direction.FORWARD, the default) fans out to keys AT AND AFTER the upstream key's anchor; 'backward' fans out to keys ENDING AT it.")

    minimum_interval_seconds: int = Field(default=30, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="stopped", description="'running' or 'stopped'.")

    @model_validator(mode="after")
    def _validate_fields(self):
        if self.window_type == "segment":
            if not self.segment_keys:
                raise ValueError("PartitionWindowSensorComponent: window_type='segment' requires `segment_keys`.")
        if self.mode == "rollup" and self.wait_policy == "minimum_count" and self.minimum_count is None:
            raise ValueError("PartitionWindowSensorComponent: wait_policy='minimum_count' requires `minimum_count`.")
        if self.mode == "fan_out" and self.window_type in ("day", "week", "month") and not self.max_downstream_keys:
            raise ValueError("PartitionWindowSensorComponent: mode='fan_out' with a time-based window_type requires `max_downstream_keys`.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Partition Window Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        upstream_key = dg.AssetKey.from_user_string(component.upstream_asset_key)
        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if component.default_status.lower() == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=component.sensor_name,
            minimum_interval_seconds=component.minimum_interval_seconds,
            default_status=default_status,
            job_name=component.job_name,
        )
        def _partition_window_sensor(sensor_context: dg.SensorEvaluationContext):
            materialized = sensor_context.instance.get_materialized_partitions(upstream_key)
            state = _load_cursor(sensor_context)

            if component.mode == "rollup":
                run_requests, state = _evaluate_rollup(component, materialized, state)
            else:
                run_requests, state = _evaluate_fan_out(component, materialized, state)

            return dg.SensorResult(
                run_requests=run_requests,
                cursor=json.dumps(state),
                skip_reason=None if run_requests else "window/wait condition not yet satisfied for any pending key.",
            )

        return dg.Definitions(sensors=[_partition_window_sensor])


def _evaluate_rollup(component: "PartitionWindowSensorComponent", materialized: Set[str], state: dict) -> Tuple[List["dg.RunRequest"], dict]:
    fired = set(state.get("fired", []))
    run_requests: List[dg.RunRequest] = []

    if component.window_type == "segment":
        if "all_segments" in fired:
            return [], state
        present = set(component.segment_keys) & materialized
        satisfied = (
            len(present) == len(component.segment_keys) if component.wait_policy == "all"
            else len(present) >= component.minimum_count
        )
        if satisfied:
            run_requests.append(dg.RunRequest(run_key=f"rollup-{component.sensor_name}-all_segments"))
            fired.add("all_segments")
        return run_requests, {"fired": sorted(fired)}

    # time-based: day / week / month
    candidates: dict = {}
    for uk in materialized:
        dt = _parse_upstream_key(uk, component.upstream_key_format)
        if dt is None:
            continue
        anchor = _rollup_anchor(dt, component.window_type)
        rollup_key = anchor.strftime(component.downstream_key_format)
        candidates.setdefault(rollup_key, anchor)

    for rollup_key, anchor in sorted(candidates.items()):
        if rollup_key in fired:
            continue
        expected = _expected_upstream_keys(anchor, component.window_type, component.upstream_key_format)
        present = expected & materialized
        satisfied = (
            len(present) == len(expected) if component.wait_policy == "all"
            else len(present) >= component.minimum_count
        )
        if satisfied:
            run_requests.append(dg.RunRequest(run_key=f"rollup-{component.sensor_name}-{rollup_key}", partition_key=rollup_key))
            fired.add(rollup_key)

    # Bound cursor growth -- keep only the most recent entries.
    fired_sorted = sorted(fired)[-500:]
    return run_requests, {"fired": fired_sorted}


def _evaluate_fan_out(component: "PartitionWindowSensorComponent", materialized: Set[str], state: dict) -> Tuple[List["dg.RunRequest"], dict]:
    seen = set(state.get("seen", []))
    new_keys = materialized - seen
    run_requests: List[dg.RunRequest] = []

    if component.window_type == "segment":
        for uk in sorted(new_keys):
            if uk not in component.segment_keys:
                continue
            run_requests.append(dg.RunRequest(run_key=f"fanout-{component.sensor_name}-{uk}", partition_key=uk))
    else:
        for uk in sorted(new_keys):
            dt = _parse_upstream_key(uk, component.upstream_key_format)
            if dt is None:
                continue
            # Unlike rollup, fan_out steps directly from the upstream key itself -- not a
            # truncated day/week/month anchor. 'forward' starts AT the key; 'backward' ends
            # AT it (matches the real example DAG's own "seven days ending at the key").
            for downstream_key in _fan_out_keys(dt, component.window_type, component.max_downstream_keys, component.direction, component.downstream_key_format):
                run_requests.append(dg.RunRequest(run_key=f"fanout-{component.sensor_name}-{uk}-{downstream_key}", partition_key=downstream_key))

    seen_sorted = sorted(seen | new_keys)[-500:]
    return run_requests, {"seen": seen_sorted}
