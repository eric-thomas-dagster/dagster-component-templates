"""External Pub/Sub Asset Component.

Set `create_observation_sensor: true` to also get the polling sensor that
`PubsubObservationSensorComponent` provides standalone -- one component,
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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
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


class ExternalPubsubAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    project_id: str = Field(description="GCP project ID")
    topic_id: str = Field(description="Pub/Sub topic ID")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current (same logic as the standalone PubsubObservationSensorComponent). "
            "When False (default), this component only declares the AssetSpec -- pair it with a "
            "separate PubsubObservationSensorComponent yourself if you want observation."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{topic_id}__observation_sensor'. Only used when create_observation_sensor=True.",
    )
    subscription_id: Optional[str] = Field(
        default=None,
        description="Subscription ID for lag metrics. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Seconds between health checks. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster resource key. Only used when create_observation_sensor=True.",
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
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

        if not self.create_observation_sensor:
            return dg.Definitions(assets=[spec])

        _self = self
        sensor_name = self.sensor_name or f"{_safe_sensor_name_part(self.topic_id)}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(dg.AssetKey.from_user_string(self.asset_key)),
        )
        def _pubsub_obs(context: dg.SensorEvaluationContext, **_resources):
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
            # ── Resource-backed path (v0.10.46) ─────────────────────────────
            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(f'projects/{_self.project_id}/topics/{_self.topic_id}'))
                except Exception as _rk_e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {_rk_e}")
                    return dg.SensorResult(skip_reason=f"resource observe failed: {_rk_e}")
                _rk_dv = str(_rk_observed.pop("data_version", ""))
                return dg.SensorResult(asset_events=[_event_cls(
                    asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                    metadata=_rk_observed,
                    tags={DATA_VERSION_TAG: _rk_dv} if _rk_dv else None,
                )])
            try:
                from google.cloud import pubsub_v1
            except ImportError:
                return dg.SensorResult(skip_reason="google-cloud-pubsub not installed")

            try:
                if resource_key:
                    publisher = getattr(context.resources, resource_key)
                else:
                    publisher = pubsub_v1.PublisherClient()
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Connect failed: {e}")

            topic_path = f"projects/{_self.project_id}/topics/{_self.topic_id}"
            obs_metadata = {"topic_path": topic_path, "project_id": _self.project_id, "topic_id": _self.topic_id}

            try:
                topic = publisher.get_topic(request={"topic": topic_path})
                obs_metadata["message_retention_duration_seconds"] = (
                    topic.message_retention_duration.seconds if topic.message_retention_duration else 0
                )
            except Exception as e:
                context.log.warning(f"Could not get topic details: {e}")

            if _self.subscription_id:
                try:
                    subscriber = pubsub_v1.SubscriberClient()
                    sub_path = f"projects/{_self.project_id}/subscriptions/{_self.subscription_id}"
                    sub = subscriber.get_subscription(request={"subscription": sub_path})
                    obs_metadata["subscription_id"] = _self.subscription_id
                except Exception as e:
                    context.log.warning(f"Could not get subscription details: {e}")

            return dg.SensorResult(asset_events=[_event_cls(
                asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                metadata=obs_metadata,
                tags={DATA_VERSION_TAG: json.dumps(obs_metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(assets=[spec], sensors=[_pubsub_obs])
