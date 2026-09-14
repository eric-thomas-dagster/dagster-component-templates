"""IntervalScheduleComponent.

Friendlier than cron when you just want "every 30 minutes". Generates a cron
expression under the hood, plus the same job + ScheduleDefinition shape as
`cron_schedule`.

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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
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


class IntervalScheduleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an asset selection at a fixed interval (every N minutes / hours / days).

    Two modes — same field surface as `cron_schedule`:
      1. Un-partitioned (default): `ScheduleDefinition(job=..., cron_schedule=…)`
         where the cron is derived from `every`.
      2. Time-partitioned (opt-in via `partition_type` / `partition_dimensions`):
         `build_schedule_from_partitioned_job`. `every` must match the partition
         cadence exactly (`every: 1h` ↔ hourly, etc.) — otherwise raises loudly.
    """

    schedule_name: str = Field(description="Unique schedule name.")
    every: str = Field(description="Interval value + unit, e.g. '15m', '2h', '1d', '1w', '1mo'.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize on each tick.")
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
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to runs created by this schedule.")

    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', "
            "'multi', 'dynamic', or None for unpartitioned. Only time-based types "
            "('hourly'/'daily'/'weekly'/'monthly') work with the partitioned-job "
            "path; 'static' / 'dynamic' raise ValueError."
        ),
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start in ISO format (e.g. '2024-01-01' or '2024-01-01-00:00' for hourly). Required for time-based types.",
    )
    partition_values: Optional[str] = Field(
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

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        _job_name = self.job_name or f"{self.schedule_name}_job"
        _default_status = (
            DefaultScheduleStatus.RUNNING
            if self.default_status.upper() == "RUNNING"
            else DefaultScheduleStatus.STOPPED
        )

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
                selection=AssetSelection.assets(*targets),
                partitions_def=partitions_def,
            )
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
                selection=AssetSelection.assets(*targets),
            )
            sched = ScheduleDefinition(
                name=self.schedule_name,
                cron_schedule=cron,
                job=job,
                execution_timezone=self.execution_timezone or "UTC",
                default_status=_default_status,
                tags=self.tags or {},
            )
        return Definitions(schedules=[sched], jobs=[job])
