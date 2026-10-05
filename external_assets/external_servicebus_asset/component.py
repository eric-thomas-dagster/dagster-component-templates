"""External Service Bus Asset Component.

Set `create_observation_sensor: true` to also get the polling sensor that
`servicebus_observation_sensor` provides standalone -- one component, one
YAML, asset + sensor wired together automatically. The standalone sensor
component is unaffected and still exists for cases where the sensor needs
to observe an asset_key defined elsewhere.
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


class ExternalServiceBusAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    namespace: str = Field(description="Azure Service Bus namespace")
    queue_name: Optional[str] = Field(default=None, description="Queue name (use queue_name OR topic_name)")
    topic_name: Optional[str] = Field(default=None, description="Topic name (use topic_name + subscription_name)")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current (same logic as the standalone servicebus_observation_sensor component). "
            "When False (default), this component only declares the AssetSpec -- pair it with a "
            "separate servicebus_observation_sensor component yourself if you want observation."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{queue_name or topic_name or namespace}__observation_sensor' (sanitized to valid sensor-name characters). Only used when create_observation_sensor=True.",
    )
    subscription_name: Optional[str] = Field(
        default=None,
        description="Subscription name (for topics). Only used when create_observation_sensor=True.",
    )
    connection_string_env_var: Optional[str] = Field(
        default=None,
        description="Env var with connection string. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=60,
        description="Seconds between health checks. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster resource key exposing `.observe(source) -> dict`. Only used when create_observation_sensor=True.",
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
        source = self.queue_name or self.topic_name or "unknown"
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
            description=self.description or f"Service Bus {self.namespace}/{source}",
            kinds={"servicebus", "azure", "queue"},
            metadata={
                "namespace": self.namespace,
                "queue_name": self.queue_name or "",
                "topic_name": self.topic_name or "",
                "dagster.observability_type": "external",
            },
            partitions_def=partitions_def,
        )

        if not self.create_observation_sensor:
            return dg.Definitions(assets=[spec])

        _self = self
        if self.sensor_name:
            sensor_name = self.sensor_name
        else:
            import re as _re
            _safe_source = _re.sub(r"[^A-Za-z0-9_]", "_", self.queue_name or self.topic_name or self.namespace)
            sensor_name = f"{_safe_source}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(dg.AssetKey.from_user_string(self.asset_key)),
        )
        def _sb_obs(context: dg.SensorEvaluationContext):
            import json as _json
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation

            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(_self.queue_name or _self.topic_name or ''))
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
                from azure.servicebus.management import ServiceBusAdministrationClient
            except ImportError:
                return dg.SensorResult(skip_reason="azure-servicebus not installed")

            try:
                if _self.connection_string_env_var:
                    import os
                    conn_str = os.environ.get(_self.connection_string_env_var, "")
                    mgmt = ServiceBusAdministrationClient.from_connection_string(conn_str)
                else:
                    from azure.identity import DefaultAzureCredential
                    mgmt = ServiceBusAdministrationClient(
                        fully_qualified_namespace=f"{_self.namespace}.servicebus.windows.net",
                        credential=DefaultAzureCredential(),
                    )
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Connect failed: {e}")

            metadata = {"namespace": _self.namespace}
            try:
                if _self.queue_name:
                    props = mgmt.get_queue_runtime_properties(_self.queue_name)
                    metadata["active_message_count"] = props.active_message_count
                    metadata["dead_letter_message_count"] = props.dead_letter_message_count
                    metadata["scheduled_message_count"] = props.scheduled_message_count
                    metadata["queue_name"] = _self.queue_name
                elif _self.topic_name and _self.subscription_name:
                    props = mgmt.get_subscription_runtime_properties(_self.topic_name, _self.subscription_name)
                    metadata["active_message_count"] = props.active_message_count
                    metadata["dead_letter_message_count"] = props.dead_letter_message_count
                    metadata["topic_name"] = _self.topic_name
                    metadata["subscription_name"] = _self.subscription_name
            except Exception as e:
                return dg.SensorResult(skip_reason=f"GetProperties failed: {e}")

            return dg.SensorResult(asset_events=[_event_cls(
                asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: _json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(assets=[spec], sensors=[_sb_obs])
