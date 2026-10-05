"""External Kafka Asset Component.

Set `create_observation_sensor: true` to also get the polling sensor that
`KafkaObservationSensorComponent` provides standalone -- one component,
one YAML, asset + sensor wired together automatically. The standalone
sensor component is unaffected and still exists for cases where the
sensor needs to observe an asset_key defined elsewhere.
"""
import json
import re
from typing import Any, Dict, List, Optional
import dagster as dg
from pydantic import Field

def _safe_sensor_name_part(s: str) -> str:
    """Sanitize a vendor identifier (which may contain '-', '.', '/', '+', etc.)
    into a valid Dagster sensor-name fragment (must match ^[A-Za-z0-9_]+$)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", s)

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


class ExternalKafkaAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses")
    topic: str = Field(description="Kafka topic name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current (same logic as the standalone KafkaObservationSensorComponent). "
            "When False (default), this component only declares the AssetSpec -- pair it with a "
            "separate KafkaObservationSensorComponent yourself if you want observation."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{topic}__observation_sensor'. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Seconds between health checks. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(topic) -> dict` that returns "
            "`{'data_version': str, **metadata}`. When set, the sensor uses this "
            "instead of connecting to bootstrap_servers directly. Only used when "
            "create_observation_sensor=True."
        ),
    )
    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), the sensor emits AssetMaterialization (asset shows healthy/green, "
            "downstream AutomationCondition.eager() fires on parent updates). When False, emits "
            "AssetObservation instead (no Dagster+ credit charge, but eager()-style conditions won't "
            "fire). Only used when create_observation_sensor=True."
        ),
    )

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
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or f"Kafka topic {self.topic}",
            kinds={"kafka", "streaming"},
            metadata={
                "bootstrap_servers": self.bootstrap_servers,
                "topic": self.topic,
                "dagster.observability_type": "external",
            },
            partitions_def=partitions_def,
        )

        if not self.create_observation_sensor:
            return dg.Definitions(assets=[spec])

        _self = self
        sensor_name = self.sensor_name or f"{_safe_sensor_name_part(self.topic)}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(dg.AssetKey.from_user_string(self.asset_key)),
        )
        def _kafka_obs(context: dg.SensorEvaluationContext, **_resources):
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
            # ── Resource-backed path (preferred) ────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    observed: dict[str, Any] = dict(client.observe(_self.topic))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return dg.SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return dg.SensorResult(asset_events=[_event_cls(
                    asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native kafka-python path ────────────────────────────────────
            try:
                from kafka import KafkaConsumer, TopicPartition
            except ImportError:
                return dg.SensorResult(skip_reason="kafka-python not installed")

            try:
                consumer = KafkaConsumer(bootstrap_servers=_self.bootstrap_servers)
                partitions = consumer.partitions_for_topic(_self.topic) or set()
                tps = [TopicPartition(_self.topic, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps) if tps else {}
                total_messages = sum(end_offsets.values())
                consumer.close()
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Offset query failed: {e}")

            # Per-partition offsets → repartition-safe DataVersion.
            offsets_by_partition = {str(tp.partition): int(off) for tp, off in end_offsets.items()}
            data_version = json.dumps(offsets_by_partition, sort_keys=True)

            obs_metadata = {
                "partition_count": len(partitions),
                "total_end_offset": total_messages,
                "topic": _self.topic,
                "bootstrap_servers": _self.bootstrap_servers,
            }
            return dg.SensorResult(asset_events=[_event_cls(
                asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                metadata=obs_metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(assets=[spec], sensors=[_kafka_obs])
