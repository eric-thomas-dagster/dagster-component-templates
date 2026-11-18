"""IntervalScheduleComponent.

Friendlier than cron when you just want "every 30 minutes". Generates a cron
expression under the hood, plus the same job + ScheduleDefinition shape as
`cron_schedule` -- including the same asset_keys/asset_check_keys surface
(materialize assets, run asset checks, or both on each tick).

Also supports the same partition surface as `cron_schedule` — declare
`partition_type` (+ related fields) and the component switches to
`build_schedule_from_partitioned_job` under the hood so each tick auto-targets
the most-recent finished partition. In the partitioned path the interval
MUST match the partition cadence (`every: 1h` ↔ `partition_type: hourly`,
`every: 1d` ↔ `daily`, etc.) — anything else is a misconfiguration and is
rejected loudly.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetCheckKey,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from pydantic import Field


# NOTE: `_build_partitions_def` is duplicated from cron_schedule/component.py
# by design — the DCC convention is that every component is self-contained
# (no shared helpers across component packages). If two components need the
# same helper, both get their own copy.

def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
    end_date=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

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
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone, end_date=end_date)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone, end_date=end_date)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone, end_date=end_date)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone, end_date=end_date)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone, end=end_date)
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


def _parse_every_to_cadence(every: str) -> tuple[str, str]:
    """Parse an `every: <N><unit>` string into (unit, cron_expression).

    Returns (unit_short, cron) where unit_short ∈ {'m','h','d','w','mo'}.
    """
    v = every.strip().lower()
    # "1mo" first (before "m") so 1mo doesn't parse as 1 minute
    if v.endswith("mo"):
        n = int(v[:-2])
        if n != 1:
            raise ValueError(f"Invalid 'every' value {every!r}: months only support '1mo'.")
        return "mo", "0 0 1 * *"
    if v.endswith("m"):
        n = int(v[:-1])
        cron = f"*/{n} * * * *" if n < 60 else f"0 */{n // 60} * * *"
        return "m", cron
    if v.endswith("h"):
        n = int(v[:-1])
        return "h", f"0 */{n} * * *"
    if v.endswith("d"):
        n = int(v[:-1])
        return "d", f"0 0 */{n} * *"
    if v.endswith("w"):
        n = int(v[:-1])
        if n != 1:
            raise ValueError(f"Invalid 'every' value {every!r}: weeks only support '1w'.")
        return "w", "0 0 * * 0"
    raise ValueError(f"Invalid 'every' value: {every!r}. Use e.g. '15m', '2h', '1d', '1w', '1mo'.")


def _validate_interval_matches_cadence(every: str, unit: str, cadence: str) -> None:
    """Enforce that `every` matches the partition cadence when partitioned.

    Only `every: 1<unit>` values map cleanly to Dagster's partitioned-job
    schedule builder (which fires exactly once per partition period).
    Anything else (every 15m + hourly partitions, every 2h + hourly, etc.)
    would over- or under-fire relative to the partitions_def.
    """
    v = every.strip().lower()
    unit_to_cadence = {"h": "hourly", "d": "daily", "w": "weekly", "mo": "monthly"}
    expected_cadence = unit_to_cadence.get(unit)
    if expected_cadence != cadence:
        raise ValueError(
            f"every={every!r} does not match partition cadence {cadence!r}. "
            f"Partitioned interval schedules require the interval to match the "
            f"partition cadence exactly: 'every: 1h' ↔ hourly, 'every: 1d' ↔ "
            f"daily, 'every: 1w' ↔ weekly, 'every: 1mo' ↔ monthly."
        )
    # Also reject 'every: 2h' + 'hourly' etc. — must be exactly '1<unit>' for
    # the partition builder to fire once per period.
    stripped = v[:-len(unit)] if unit != "mo" else v[:-2]
    if stripped != "1":
        raise ValueError(
            f"every={every!r} must be '1{unit}' (exactly one period) when "
            f"partitioned — the schedule fires once per partition, so a "
            f"multi-period interval is a misconfiguration."
        )


def _parse_iso_datetime(value: str):
    """Parse an end_date field value. Accepts a bare date ('2026-06-01') or a full
    ISO datetime; a value with no timezone is assumed UTC, matching this component's
    other timezone-less string fields."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _build_once_sensor(schedule_name: str, job, default_status, tags):
    """cadence='once': a sensor with a persistent cursor flag -- fires a single
    RunRequest the first time it evaluates while RUNNING, then SkipReason forever
    after. Verified live: tick 1 returns a RunRequest and sets the cursor; tick 2+
    (even across a fresh SensorEvaluationContext given the same cursor) returns
    SkipReason. This mirrors Airflow's @once schedule -- a one-time trigger, not a
    recurring cadence -- so it builds a sensor, not a ScheduleDefinition."""
    import dagster as dg

    @dg.sensor(job=job, name=f"{schedule_name}_sensor", default_status=default_status)
    def _once_sensor(context: dg.SensorEvaluationContext):
        if context.cursor == "fired":
            return dg.SkipReason(
                "cadence='once': already fired its single run; this sensor never "
                "fires again (mirrors Airflow's @once schedule)."
            )
        context.update_cursor("fired")
        return dg.RunRequest(run_key=schedule_name, tags=tags or {})

    return _once_sensor



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


class IntervalScheduleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an asset and/or asset check selection at a fixed interval (every N minutes / hours / days).

    Two modes — same field surface as `cron_schedule`:
      1. Un-partitioned (default): `ScheduleDefinition(job=..., cron_schedule=…)`
         where the cron is derived from `every`.
      2. Time-partitioned (opt-in via `partition_type` / `partition_dimensions`):
         `build_schedule_from_partitioned_job`. `every` must match the partition
         cadence exactly (`every: 1h` ↔ hourly, etc.) — otherwise raises loudly.
    """

    schedule_name: str = Field(description="Unique schedule name.")
    every: Optional[str] = Field(
        default=None,
        description=(
            "Interval value + unit, e.g. '15m', '2h', '1d', '1w', '1mo'. "
            "Required when cadence='scheduled' (the default); must be unset "
            "when cadence='once'."
        ),
    )
    asset_keys: Optional[List[str]] = Field(
        default=None,
        description="Slash-separated asset keys to materialize on each tick. Set this, asset_check_keys, or both.",
    )
    asset_check_keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "Asset check keys to run on each tick, as '<asset_key>:<check_name>' "
            "(asset key segments slash-separated, e.g. 'marts/orders:row_count_check'). "
            "Runs just the check -- via Dagster's real AssetSelection.checks(...), "
            "confirmed to execute with zero re-materialization of the parent asset "
            "-- for periodic re-validation without re-running the (possibly "
            "expensive) materialization. Combine with asset_keys to also "
            "materialize other assets in the same tick."
        ),
    )
    job_name: Optional[str] = Field(default=None, description="Name of the underlying job (defaults to '<schedule_name>_job').")
    execution_timezone: Optional[str] = Field(
        default=None,
        description=(
            "IANA timezone, e.g. 'America/New_York'. Applied in the un-partitioned "
            "path (Dagster falls back to UTC when None); silently IGNORED in the "
            "partitioned path — build_schedule_from_partitioned_job forbids "
            "execution_timezone for time-partitioned jobs and uses the "
            "partitions_def's own timezone instead."
        ),
    )
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Tags applied to runs created by this schedule.")

    cadence: str = Field(
        default="scheduled",
        description=(
            "'scheduled' (default): normal fixed-interval schedule, as below. "
            "'once': mirrors Airflow's @once -- fires a single run the first "
            "time this component's sensor evaluates while RUNNING, then never "
            "fires again. Builds a sensor instead of a schedule, so it's "
            "mutually exclusive with every, partition_type/partition_dimensions, "
            "end_date, and add_logical_date_tag."
        ),
    )
    end_date: Optional[str] = Field(
        default=None,
        description=(
            "ISO date or datetime (e.g. '2026-06-01'; a value with no timezone "
            "is assumed UTC) after which this schedule stops firing -- mirrors "
            "Airflow's DAG end_date. Un-partitioned: each tick is skipped once "
            "scheduled_execution_time reaches this date. Partitioned: ALSO "
            "passed to the partitions_def's own end_date, in addition to the "
            "same tick-level skip. Only valid with cadence='scheduled'."
        ),
    )
    add_logical_date_tag: bool = Field(
        default=False,
        description=(
            "If true, tags each run with 'logical_date' -- Airflow's run-"
            "identifying timestamp. Partitioned: the partition window's start. "
            "Un-partitioned: the scheduled trigger time. Only valid with "
            "cadence='scheduled'."
        ),
    )

    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', "
            "'multi', 'dynamic', or None for unpartitioned. Only time-based types "
            "('hourly'/'daily'/'weekly'/'monthly') work with the partitioned-job "
            "path; 'static' / 'dynamic' raise ValueError."
        ),
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start in ISO format (e.g. '2024-01-01' or '2024-01-01-00:00' for hourly). Required for time-based types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for multi-axis partitioning (unused in flat time-based mode).",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (rejected by this schedule — declared for parity with cron_schedule).",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="after")
    def _validate_cadence(self):
        if self.cadence not in ("scheduled", "once"):
            raise ValueError(f"cadence must be 'scheduled' or 'once', got {self.cadence!r}.")
        if self.cadence == "once":
            if self.every:
                raise ValueError("cadence='once' fires via a one-time sensor, not a recurring interval -- unset every.")
            if self.partition_type or self.partition_dimensions:
                raise ValueError("cadence='once' fires a single ad hoc run with no partition key -- unset partition_type/partition_dimensions.")
            if self.end_date:
                raise ValueError("cadence='once' has no recurring ticks to stop -- unset end_date.")
            if self.add_logical_date_tag:
                raise ValueError("cadence='once' has no scheduled trigger time or partition window to tag -- unset add_logical_date_tag.")
        elif not self.every:
            raise ValueError("every is required when cadence='scheduled' (the default).")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Interval Schedule', editable=True)

    @staticmethod
    def _parse_check_key(raw: str) -> AssetCheckKey:
        """Parse '<asset_key>:<check_name>' (asset key segments slash-separated)
        into a real AssetCheckKey. Splits on the LAST colon so a check name
        can't accidentally swallow part of the asset key."""
        asset_part, sep, check_name = raw.rpartition(":")
        if not sep or not asset_part or not check_name:
            raise ValueError(
                f"asset_check_keys entry {raw!r} must be '<asset_key>:<check_name>', "
                f"e.g. 'marts/orders:row_count_check'."
            )
        return AssetCheckKey(AssetKey.from_user_string(asset_part), check_name)

    def _build_selection(self, targets: List[AssetKey]) -> AssetSelection:
        """Union of the asset selection and the check selection -- confirmed
        via dg.AssetSelection.checks(...) that a check-only selection runs
        just the check, with zero re-materialization of its parent asset."""
        selection = AssetSelection.assets(*targets) if targets else None
        if self.asset_check_keys:
            check_keys = [self._parse_check_key(k) for k in self.asset_check_keys]
            check_selection = AssetSelection.checks(*check_keys)
            selection = check_selection if selection is None else selection | check_selection
        if selection is None:
            raise ValueError("Set at least one of asset_keys or asset_check_keys.")
        return selection

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        targets = [AssetKey.from_user_string(k) for k in (self.asset_keys or [])]
        selection = self._build_selection(targets)
        _job_name = self.job_name or f"{self.schedule_name}_job"
        _default_status = (
            DefaultScheduleStatus.RUNNING
            if self.default_status.upper() == "RUNNING"
            else DefaultScheduleStatus.STOPPED
        )

        if self.cadence == "once":
            job = define_asset_job(name=_job_name, selection=selection)
            _default_sensor_status = (
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            )
            sensor = _build_once_sensor(self.schedule_name, job, _default_sensor_status, self.tags)
            return Definitions(sensors=[sensor], jobs=[job])

        _end_dt = _parse_iso_datetime(self.end_date) if self.end_date else None
        _advanced = bool(_end_dt or self.add_logical_date_tag)

        unit, cron = _parse_every_to_cadence(self.every)
        _is_partitioned = bool(self.partition_type or self.partition_dimensions)

        if _is_partitioned:
            if self.partition_type in ("static", "dynamic"):
                raise ValueError(
                    f"partition_type={self.partition_type!r} is not supported "
                    f"by IntervalScheduleComponent — build_schedule_from_"
                    f"partitioned_job requires a time-based partitions_def "
                    f"(daily/weekly/monthly/hourly)."
                )

            partitions_def = _build_partitions_def(
                self.partition_type,
                self.partition_start,
                self.partition_values,
                self.dynamic_partition_name,
                self.partition_dimensions,

    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
    end_date=self.end_date,
)

            _cadence = self.partition_type if self.partition_type in (
                "hourly", "daily", "weekly", "monthly"
            ) else None
            if _cadence is None and self.partition_dimensions:
                _time_axes = [
                    d.get("type") for d in self.partition_dimensions
                    if d.get("type") in ("hourly", "daily", "weekly", "monthly")
                ]
                _cadence = _time_axes[0] if _time_axes else None

            if _cadence:
                _validate_interval_matches_cadence(self.every, unit, _cadence)

            job = define_asset_job(
                name=_job_name,
                selection=selection,
                partitions_def=partitions_def,
            )

            if _advanced:
                # end_date / add_logical_date_tag need a per-tick execution_fn
                # (to skip past end_date, and/or to tag the resolved window's
                # start) -- build_schedule_from_partitioned_job below has
                # neither hook, so this path resolves the partition window
                # itself instead. Same mechanism as cron_schedule's advanced
                # partitioned path, verified live there.
                _pd, _end, _tag, _tags = partitions_def, _end_dt, self.add_logical_date_tag, self.tags

                def _execution_fn(sched_context, _pd=_pd, _end=_end, _tag=_tag, _tags=_tags):
                    if _end and sched_context.scheduled_execution_time >= _end:
                        return dg.SkipReason(f"past end_date {_end.isoformat()}")
                    window = _pd.get_last_partition_window()
                    if window is None:
                        return dg.SkipReason("no completed partition yet")
                    keys = _pd.get_partition_keys_in_time_window(window)
                    if not keys:
                        return dg.SkipReason("no partition keys in the most recent completed window")
                    merged_tags = dict(_tags or {})
                    if _tag:
                        merged_tags["logical_date"] = window.start.isoformat()
                    return dg.RunRequest(partition_key=keys[-1], tags=merged_tags)

                sched = ScheduleDefinition(
                    name=self.schedule_name,
                    cron_schedule=cron,
                    job=job,
                    execution_timezone=self.execution_timezone or "UTC",
                    execution_fn=_execution_fn,
                    default_status=_default_status,
                )
            else:
                # build_schedule_from_partitioned_job REJECTS cron_schedule and
                # execution_timezone for time-partitioned jobs. Cadence is inferred
                # from partitions_def; we already validated that `every` matches
                # that cadence, so the natural period boundary (fire at :00) is
                # exactly what the user expects.
                sched = build_schedule_from_partitioned_job(
                    job=job,
                    name=self.schedule_name,
                    default_status=_default_status,
                    tags=self.tags or {},
                )
        else:
            job = define_asset_job(
                name=_job_name,
                selection=selection,
            )
            if _advanced:
                _end, _tag, _tags = _end_dt, self.add_logical_date_tag, self.tags

                def _execution_fn(sched_context, _end=_end, _tag=_tag, _tags=_tags):
                    if _end and sched_context.scheduled_execution_time >= _end:
                        return dg.SkipReason(f"past end_date {_end.isoformat()}")
                    merged_tags = dict(_tags or {})
                    if _tag:
                        merged_tags["logical_date"] = sched_context.scheduled_execution_time.isoformat()
                    return dg.RunRequest(tags=merged_tags)

                sched = ScheduleDefinition(
                    name=self.schedule_name,
                    cron_schedule=cron,
                    job=job,
                    execution_timezone=self.execution_timezone or "UTC",
                    execution_fn=_execution_fn,
                    default_status=_default_status,
                )
            else:
                sched = ScheduleDefinition(
                    name=self.schedule_name,
                    cron_schedule=cron,
                    job=job,
                    execution_timezone=self.execution_timezone or "UTC",
                    default_status=_default_status,
                    tags=self.tags or {},
                )
        return Definitions(schedules=[sched], jobs=[job])
