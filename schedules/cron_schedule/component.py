"""CronScheduleComponent.

Defines a `ScheduleDefinition` + an underlying asset job that materializes
the configured asset keys whenever the cron expression fires. Replaces
the ad-hoc `dg launch --assets '*'` pattern with a real Dagster schedule
that shows up in the UI.

When time-based partition fields are supplied, the component switches
to `build_schedule_from_partitioned_job` so each tick auto-targets the
most-recent finished partition — `ScheduleDefinition` against a
partitioned job errors with "no partition key provided" on every tick.
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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
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
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
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


def _validate_cron_for_cadence(cron_expression: str, cadence: str) -> None:
    """Loosely validate a user-supplied cron against a partitions_def cadence.

    Strict equality is overkill — many cron values fire once per period.
    What we check is that the cron's tick-rate matches the partitions_def:
      hourly  → cron fires once/hour: minute fixed, hour wild
      daily   → cron fires once/day:  minute + hour both fixed
      weekly  → cron fires once/week: minute + hour fixed + day-of-week fixed
      monthly → cron fires once/month: minute + hour + day-of-month fixed

    Raises ValueError on mismatch so misconfigurations are loud, not silent.
    """
    parts = cron_expression.split()
    if len(parts) != 5:
        raise ValueError(
            f"cron_expression must have 5 fields (m h dom mon dow); got "
            f"{len(parts)}: {cron_expression!r}"
        )
    minute, hour, dom, _mon, dow = parts

    def _wild(field: str) -> bool:
        return field == "*" or field.startswith("*/")

    def _fixed(field: str) -> bool:
        return not _wild(field)

    if cadence == "hourly":
        if not (_fixed(minute) and _wild(hour)):
            raise ValueError(
                f"cron_expression {cron_expression!r} fires more (or less) than "
                f"once per hour but partitions_def is hourly. Expected "
                f"'M * * * *' shape."
            )
    elif cadence == "daily":
        if not (_fixed(minute) and _fixed(hour) and _wild(dom) and _wild(dow)):
            raise ValueError(
                f"cron_expression {cron_expression!r} doesn't fire once per day "
                f"but partitions_def is daily. Expected 'M H * * *' shape."
            )
    elif cadence == "weekly":
        if not (_fixed(minute) and _fixed(hour) and _wild(dom) and _fixed(dow)):
            raise ValueError(
                f"cron_expression {cron_expression!r} doesn't fire once per week "
                f"but partitions_def is weekly. Expected 'M H * * D' shape."
            )
    elif cadence == "monthly":
        if not (_fixed(minute) and _fixed(hour) and _fixed(dom) and _wild(dow)):
            raise ValueError(
                f"cron_expression {cron_expression!r} doesn't fire once per month "
                f"but partitions_def is monthly. Expected 'M H D * *' shape."
            )


class CronScheduleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an asset selection on a cron schedule.

    Two modes — both share the same field surface:
      1. Un-partitioned (default): `ScheduleDefinition(job=..., cron_schedule=...)`.
         `cron_expression` is required.
      2. Time-partitioned (opt-in via `partition_type`/`partition_dimensions`):
         `build_schedule_from_partitioned_job`. `cron_expression` becomes
         optional — Dagster infers cadence from the partitions_def. Each
         tick auto-targets the most-recent finished partition.
    """

    schedule_name: str = Field(description="Unique schedule name shown in the UI.")
    cron_expression: Optional[str] = Field(
        default=None,
        description=(
            "Cron expression (5 fields: m h dom mon dow). E.g. '0 9 * * *' "
            "= daily 09:00. REQUIRED when no partition fields are set; "
            "OPTIONAL in the partitioned-job path (cadence is inferred from "
            "the partitions_def). If supplied in the partitioned path, "
            "validated against the partitions_def cadence."
        ),
    )
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize on each tick.")
    job_name: Optional[str] = Field(default=None, description="Name of the underlying job (defaults to '<schedule_name>_job').")
    execution_timezone: str = Field(default="UTC", description="IANA timezone for cron evaluation, e.g. 'America/Los_Angeles'.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' (live) or 'STOPPED' (dormant; user must enable).")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to runs created by this schedule.")

    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily', 'weekly', 'monthly', 'hourly', "
            "'static', 'multi', 'dynamic', or None for unpartitioned. "
            "Note: only time-based types ('daily'/'weekly'/'monthly'/"
            "'hourly') work with this schedule's partitioned-job path — "
            "'static' / 'dynamic' raise ValueError."
        ),
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start in ISO format (e.g. '2024-01-01' or '2024-01-01-00:00' for hourly). Required for time-based types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning. Only used if you also set partition_type='multi' (and even then the schedule rejects pure static — see partition_type).",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'). NOTE: the schedule rejects dynamic-only partitions.",
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

        _is_partitioned = bool(self.partition_type or self.partition_dimensions)

        if _is_partitioned:
            # build_schedule_from_partitioned_job is for time-based
            # partitions only. Surface static/dynamic loudly instead of
            # building a schedule that errors at tick time.
            if self.partition_type in ("static", "dynamic"):
                raise ValueError(
                    f"partition_type={self.partition_type!r} is not supported "
                    f"by CronScheduleComponent — build_schedule_from_partitioned_job "
                    f"requires a time-based partitions_def "
                    f"(daily/weekly/monthly/hourly). For static/dynamic "
                    f"partitions, use the un-partitioned schedule path and "
                    f"select specific partition keys downstream."
                )

            partitions_def = _build_partitions_def(
                self.partition_type,
                self.partition_start,
                self.partition_values,
                self.dynamic_partition_name,
                self.partition_dimensions,
            )

            # Validate user-supplied cron against the partitions_def cadence.
            # Inferred from partition_type (multi-dimension cadence is the
            # cadence of the time axis if present; otherwise skip the check).
            _cadence = self.partition_type if self.partition_type in (
                "hourly", "daily", "weekly", "monthly"
            ) else None
            if _cadence is None and self.partition_dimensions:
                _time_axes = [
                    d.get("type") for d in self.partition_dimensions
                    if d.get("type") in ("hourly", "daily", "weekly", "monthly")
                ]
                _cadence = _time_axes[0] if _time_axes else None

            if self.cron_expression and _cadence:
                _validate_cron_for_cadence(self.cron_expression, _cadence)

            job = define_asset_job(
                name=_job_name,
                selection=AssetSelection.assets(*targets),
                partitions_def=partitions_def,
            )
            sched = build_schedule_from_partitioned_job(
                job=job,
                name=self.schedule_name,
                default_status=_default_status,
                tags=self.tags or {},
                # cron_schedule is inferred from partitions_def cadence when
                # this kwarg is omitted; pass it through when the user
                # specified one so a non-default firing minute/hour
                # (e.g. '15 * * * *' for hourly partitions firing at :15)
                # propagates to the schedule.
                **({"cron_schedule": self.cron_expression} if self.cron_expression else {}),
            )
        else:
            if not self.cron_expression:
                raise ValueError(
                    "cron_expression is required when no partition fields are set."
                )
            job = define_asset_job(
                name=_job_name,
                selection=AssetSelection.assets(*targets),
            )
            sched = ScheduleDefinition(
                name=self.schedule_name,
                cron_schedule=self.cron_expression,
                job=job,
                execution_timezone=self.execution_timezone,
                default_status=_default_status,
                tags=self.tags or {},
            )
        return Definitions(schedules=[sched], jobs=[job])
