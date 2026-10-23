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


class ExternalKafkaAsset(dg.Component, dg.Model, dg.Resolvable):
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster asset key for a single topic. Mutually exclusive with `topics:` -- use "
            "this (with `topic:`) to declare exactly one topic, or `topics:` to declare several "
            "from one component instance sharing bootstrap_servers and one observation sensor."
        ),
    )
    topic: Optional[str] = Field(
        default=None,
        description="Kafka topic name for a single topic. Mutually exclusive with `topics:`.",
    )
    topics: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several Kafka topics as external assets from one component instance, "
            "instead of one ExternalKafkaAsset YAML block (and, with "
            "create_observation_sensor=true, one polling sensor and one Kafka connection) per "
            "topic. Each entry: `key` (Dagster asset key, required), `topic` (Kafka topic name, "
            "required), plus optional `deps` (list of upstream asset key strings), `owners` "
            "(list of strings), `metadata` (dict), `group_name`, `description`. All entries "
            "share this component's bootstrap_servers, partitioning config, and (if enabled) ONE "
            "sensor that polls every topic's offsets over a single Kafka connection per tick -- "
            "set `sensor_name` explicitly when using `topics:` with create_observation_sensor=true "
            "(there's no single topic name to derive a default from). Mutually exclusive with the "
            "top-level `asset_key`/`topic` fields."
        ),
    )
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
        description=(
            "Unique sensor name. Defaults to '{topic}__observation_sensor' in single-topic mode "
            "(asset_key/topic). Required when using `topics:` (no single topic name to derive a "
            "default from). Only used when create_observation_sensor=True."
        ),
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
    partition_values: Optional[Union[str, List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=('partition_values',), tags=())

    def _resolve_topic_entries(self) -> List[Dict[str, Any]]:
        """Normalize the single-topic (asset_key/topic) and multi-topic (topics:) shapes
        into one list of `{key, topic, deps, owners, metadata, group_name, description}` dicts.
        """
        has_single = bool(self.asset_key or self.topic)
        has_multi = bool(self.topics)
        if has_single and has_multi:
            raise ValueError(
                "Set either asset_key/topic (single topic) or topics: (several topics), not both."
            )
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/topic (single topic) or topics: (several topics).")

        if has_single:
            if not self.asset_key or not self.topic:
                raise ValueError("Both asset_key and topic are required in single-topic mode.")
            return [{
                "key": self.asset_key,
                "topic": self.topic,
                "deps": None,
                "owners": None,
                "metadata": None,
                "group_name": self.group_name,
                "description": self.description,
            }]

        entries = []
        seen_keys = set()
        for i, raw in enumerate(self.topics):
            if not isinstance(raw, dict):
                raise ValueError(f"topics[{i}] must be a mapping, got {type(raw).__name__}")
            key = raw.get("key")
            topic = raw.get("topic")
            if not key:
                raise ValueError(f"topics[{i}] is missing required field 'key'")
            if not topic:
                raise ValueError(f"topics[{i}] ({key!r}) is missing required field 'topic'")
            if key in seen_keys:
                raise ValueError(f"topics: duplicate key {key!r}")
            seen_keys.add(key)
            entries.append({
                "key": key,
                "topic": topic,
                "deps": raw.get("deps"),
                "owners": raw.get("owners"),
                "metadata": raw.get("metadata"),
                "group_name": raw.get("group_name", self.group_name),
                "description": raw.get("description"),
            })
        return entries

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        entries = self._resolve_topic_entries()
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        specs = []
        for e in entries:
            metadata = {
                "bootstrap_servers": self.bootstrap_servers,
                "topic": e["topic"],
                "dagster.observability_type": "external",
            }
            if e["metadata"]:
                metadata.update(e["metadata"])
            specs.append(dg.AssetSpec(
                key=dg.AssetKey.from_user_string(e["key"]),
                group_name=e["group_name"],
                description=e["description"] or f"Kafka topic {e['topic']}",
                kinds={"kafka", "streaming"},
                deps=[dg.AssetKey.from_user_string(d) for d in e["deps"]] if e["deps"] else None,
                owners=e["owners"] or None,
                metadata=metadata,
                partitions_def=partitions_def,
            ))

        if not self.create_observation_sensor:
            return dg.Definitions(assets=specs)

        is_multi = self.topics is not None
        if is_multi and not self.sensor_name:
            raise ValueError(
                "sensor_name is required when using topics: with create_observation_sensor=true "
                "(there's no single topic name to derive a default from)."
            )
        sensor_name = self.sensor_name or f"{_safe_sensor_name_part(entries[0]['topic'])}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        _entries = entries
        _self = self

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.assets(*[dg.AssetKey.from_user_string(e["key"]) for e in _entries]),
        )
        def _kafka_obs(context: dg.SensorEvaluationContext, **_resources):
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
            events = []

            # ── Resource-backed path (preferred): one client.observe() call per topic ──
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                for e in _entries:
                    try:
                        observed: dict[str, Any] = dict(client.observe(e["topic"]))
                    except Exception as ex:
                        context.log.error(f"resource '{resource_key}'.observe({e['topic']!r}) failed: {ex}")
                        continue
                    data_version = str(observed.pop("data_version", ""))
                    events.append(_event_cls(
                        asset_key=dg.AssetKey.from_user_string(e["key"]),
                        metadata=observed,
                        tags={DATA_VERSION_TAG: data_version} if data_version else None,
                    ))
                if not events:
                    return dg.SensorResult(skip_reason="no topics observed this tick")
                return dg.SensorResult(asset_events=events)

            # ── Native kafka-python path: ONE connection, polled for every topic ──
            try:
                from kafka import KafkaConsumer, TopicPartition
            except ImportError:
                return dg.SensorResult(skip_reason="kafka-python not installed")

            try:
                consumer = KafkaConsumer(bootstrap_servers=_self.bootstrap_servers)
            except Exception as ex:
                return dg.SensorResult(skip_reason=f"Kafka connection failed: {ex}")

            try:
                for e in _entries:
                    topic = e["topic"]
                    try:
                        partitions = consumer.partitions_for_topic(topic) or set()
                        tps = [TopicPartition(topic, p) for p in partitions]
                        end_offsets = consumer.end_offsets(tps) if tps else {}
                    except Exception as ex:
                        context.log.error(f"Offset query for topic {topic!r} failed: {ex}")
                        continue
                    total_messages = sum(end_offsets.values())
                    # Per-partition offsets → repartition-safe DataVersion.
                    offsets_by_partition = {str(tp.partition): int(off) for tp, off in end_offsets.items()}
                    data_version = json.dumps(offsets_by_partition, sort_keys=True)
                    obs_metadata = {
                        "partition_count": len(partitions),
                        "total_end_offset": total_messages,
                        "topic": topic,
                        "bootstrap_servers": _self.bootstrap_servers,
                    }
                    events.append(_event_cls(
                        asset_key=dg.AssetKey.from_user_string(e["key"]),
                        metadata=obs_metadata,
                        tags={DATA_VERSION_TAG: data_version},
                    ))
            finally:
                consumer.close()

            if not events:
                return dg.SensorResult(skip_reason="no topics observed this tick")
            return dg.SensorResult(asset_events=events)

        return dg.Definitions(assets=specs, sensors=[_kafka_obs])
