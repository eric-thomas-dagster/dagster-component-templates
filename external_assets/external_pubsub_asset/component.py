"""External Pub/Sub Asset Component."""
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
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    def _build_axis(spec):
        t = spec.get("type")
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
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition dimension requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "static":
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start or "2024-01-01"),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ExternalPubsubAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    project_id: str = Field(description="GCP project ID")
    topic_id: str = Field(description="Pub/Sub topic ID")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01').",
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
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Pub/Sub topic {self.project_id}/{self.topic_id}",
            kinds={"pubsub", "gcp", "streaming"},
            metadata={
                "project_id": self.project_id,
                "topic_id": self.topic_id,
                "topic_path": f"projects/{self.project_id}/topics/{self.topic_id}",
                "dagster.observability_type": "external",
            },
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
