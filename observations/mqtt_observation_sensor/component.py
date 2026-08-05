"""MQTT Observation Sensor Component."""
import threading
from typing import Optional
import dagster as dg
import json
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field

class MqttObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalMqttAsset to observe")
    broker_host: str = Field(description="MQTT broker hostname")
    topic: str = Field(description="MQTT topic to subscribe to")
    broker_port: int = Field(default=1883, description="MQTT broker port")
    connect_timeout_seconds: float = Field(default=5.0, description="Seconds to wait for connection")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. External assets show healthy/green in the Dagster UI "
            "and downstream AutomationCondition.eager() fires naturally on "
            "parent updates. When False, emit AssetObservation — free of "
            "Dagster+ credit charges, but the target asset renders as "
            "observed-external (dashed border, gray) and downstream "
            "conditions that gate on ~any_deps_missing() (including "
            "eager()) will not fire. Both event types carry the same "
            "dagster/data_version tag."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey.from_user_string(_self.asset_key)
            ),
        )
        def _mqtt_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            # ── Resource-backed path (v0.10.46) ─────────────────────────────
            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(_self.topic))
                except Exception as _rk_e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {_rk_e}")
                    return SensorResult(skip_reason=f"resource observe failed: {_rk_e}")
                _rk_dv = str(_rk_observed.pop("data_version", ""))
                return SensorResult(asset_events=[_event_cls(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=_rk_observed,
                    tags={DATA_VERSION_TAG: _rk_dv} if _rk_dv else None,
                )])
            try:
                import paho.mqtt.client as mqtt
            except ImportError:
                return SensorResult(skip_reason="paho-mqtt not installed")

            connected = threading.Event()
            broker_reachable = False
            broker_version = ""

            def on_connect(client, userdata, flags, rc):
                nonlocal broker_reachable, broker_version
                if rc == 0:
                    broker_reachable = True
                connected.set()

            if resource_key:
                # resource is expected to be a connected client
                broker_reachable = True
            else:
                try:
                    client = mqtt.Client()
                    client.on_connect = on_connect
                    client.connect(_self.broker_host, _self.broker_port, keepalive=10)
                    client.loop_start()
                    connected.wait(timeout=_self.connect_timeout_seconds)
                    client.loop_stop()
                    client.disconnect()
                except Exception as e:
                    pass

            metadata = {
                "broker_reachable": broker_reachable,
                "broker_host": _self.broker_host,
                "broker_port": _self.broker_port,
                "topic": _self.topic,
            }
            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(sensors=[_mqtt_obs])
