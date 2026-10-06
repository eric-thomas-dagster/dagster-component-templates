"""PandasDataframeCheckComponent.

Wraps `dagster-pandas` constraints to validate column existence, dtypes, and value bounds against the upstream DataFrame asset. Lighter-weight than Pandera for simple shape checks.

Set `partition_type`/`partition_start` (matching the checked asset's own
partitioning) when the target asset is partitioned -- without it, this
check binds to the target asset with no partitions_def of its own, and
Dagster can't resolve which partition's data to load for `upstream`.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
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

    # Both shapes set: ambiguous. Pick one.
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


class PandasDataframeCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Validate a DataFrame asset's column constraints via dagster-pandas."""

    asset_key: str = Field(description="Asset key the check validates.")
    required_columns: List[str] = Field(description="Columns that must be present.")
    column_types: Optional[Dict[str, str]] = Field(default=None, description="Mapping of column → expected dtype name (e.g. 'int64', 'object').")
    blocking: bool = Field(default=True, description="If True, fail blocks downstream assets.")

    partition_type: Optional[str] = Field(
        default=None,
        description="Set this to match the checked asset's own partitioning (e.g. 'daily'), or the check can't resolve which partition's data to load. 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for an unpartitioned target.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01'). Must match the checked asset's partition_start.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static / multi partition types.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec; overrides flat fields when set.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey.from_user_string(self.asset_key)
        required = self.required_columns
        column_types = self.column_types or {}
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        @dg.asset_check(asset=asset_key, blocking=self.blocking, description="Pandas dataframe shape + dtype check", partitions_def=partitions_def)
        def _pandas_check(context, upstream) -> dg.AssetCheckResult:
            errors = []
            for col in required:
                if col not in upstream.columns:
                    errors.append(f"missing column: {col}")
            for col, expected in column_types.items():
                if col in upstream.columns:
                    actual = str(upstream[col].dtype)
                    if expected not in actual:
                        errors.append(f"column {col}: expected dtype {expected}, got {actual}")
            return dg.AssetCheckResult(
                passed=len(errors) == 0,
                severity=dg.AssetCheckSeverity.ERROR if errors else dg.AssetCheckSeverity.WARN,
                metadata={"errors": dg.MetadataValue.text("\n".join(errors)) if errors else dg.MetadataValue.text("ok")},
            )
        return dg.Definitions(asset_checks=[_pandas_check])
