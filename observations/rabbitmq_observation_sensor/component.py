"""RabbitMQ Observation Sensor Component."""
from typing import Optional
import dagster as dg
import json
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field

class RabbitmqObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalRabbitmqAsset to observe")
    host: str = Field(description="RabbitMQ host")
    queue_name: str = Field(description="RabbitMQ queue name")
    virtual_host: str = Field(default="/", description="RabbitMQ virtual host")
    port: int = Field(default=5672, description="AMQP port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password")
    check_interval_seconds: int = Field(default=60, description="Seconds between health checks")
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
        def _rabbit_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            # ── Resource-backed path (v0.10.46) ─────────────────────────────
            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(_self.queue_name))
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
                import pika
            except ImportError:
                return SensorResult(skip_reason="pika not installed")

            import os
            username = os.environ.get(_self.username_env_var, "guest") if _self.username_env_var else "guest"
            password = os.environ.get(_self.password_env_var, "guest") if _self.password_env_var else "guest"

            try:
                if resource_key:
                    conn = getattr(context.resources, resource_key)
                    ch = conn.channel()
                else:
                    creds = pika.PlainCredentials(username, password)
                    params = pika.ConnectionParameters(
                        host=_self.host, port=_self.port,
                        virtual_host=_self.virtual_host, credentials=creds,
                        socket_timeout=5,
                    )
                    conn = pika.BlockingConnection(params)
                    ch = conn.channel()
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                q = ch.queue_declare(queue=_self.queue_name, passive=True)
                message_count = q.method.message_count
                consumer_count = q.method.consumer_count
                conn.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Queue declare failed: {e}")

            metadata = {
                "message_count": message_count,
                "consumer_count": consumer_count,
                "queue_name": _self.queue_name,
                "host": _self.host,
            }
            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(sensors=[_rabbit_obs])
