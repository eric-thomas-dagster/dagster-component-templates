"""Service Bus Observation Sensor Component."""
from typing import Optional
import dagster as dg
import json
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field

class ServiceBusObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalServiceBusAsset to observe")
    namespace: str = Field(description="Azure Service Bus namespace")
    queue_name: Optional[str] = Field(default=None, description="Queue name (queue_name OR topic_name)")
    topic_name: Optional[str] = Field(default=None, description="Topic name")
    subscription_name: Optional[str] = Field(default=None, description="Subscription name (for topics)")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with connection string")
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
        def _sb_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            # ── Resource-backed path (v0.10.46) ─────────────────────────────
            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(_self.queue_name or _self.topic_name or ''))
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
                from azure.servicebus.management import ServiceBusAdministrationClient
            except ImportError:
                return SensorResult(skip_reason="azure-servicebus not installed")

            try:
                if resource_key:
                    mgmt = getattr(context.resources, resource_key)
                elif _self.connection_string_env_var:
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
                return SensorResult(skip_reason=f"Connect failed: {e}")

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
                return SensorResult(skip_reason=f"GetProperties failed: {e}")

            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(sensors=[_sb_obs])
