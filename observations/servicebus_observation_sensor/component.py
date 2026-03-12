"""Service Bus Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            monitored_assets=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _sb_obs(context: SensorEvaluationContext):
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

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_sb_obs])
