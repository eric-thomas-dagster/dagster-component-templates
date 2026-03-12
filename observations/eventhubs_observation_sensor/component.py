"""Event Hubs Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class EventHubsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalEventHubsAsset to observe")
    namespace: str = Field(description="Azure Event Hubs namespace")
    eventhub_name: str = Field(description="Event Hub name")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with connection string")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
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
        def _eventhubs_obs(context: SensorEvaluationContext):
            try:
                from azure.eventhub import EventHubConsumerClient
            except ImportError:
                return SensorResult(skip_reason="azure-eventhub not installed")

            try:
                if resource_key:
                    client = getattr(context.resources, resource_key)
                elif _self.connection_string_env_var:
                    import os
                    conn_str = os.environ.get(_self.connection_string_env_var, "")
                    client = EventHubConsumerClient.from_connection_string(
                        conn_str, consumer_group="$Default", eventhub_name=_self.eventhub_name)
                else:
                    from azure.identity import DefaultAzureCredential
                    fully_qualified_namespace = f"{_self.namespace}.servicebus.windows.net"
                    client = EventHubConsumerClient(
                        fully_qualified_namespace=fully_qualified_namespace,
                        eventhub_name=_self.eventhub_name,
                        consumer_group="$Default",
                        credential=DefaultAzureCredential(),
                    )
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                partition_ids = client.get_partition_ids()
                partition_count = len(partition_ids)
                props = []
                for pid in partition_ids:
                    try:
                        p = client.get_partition_properties(pid)
                        props.append(p)
                    except Exception:
                        pass
                client.close()
            except Exception as e:
                return SensorResult(skip_reason=f"GetPartitions failed: {e}")

            last_seq = max((p.get("last_enqueued_sequence_number", 0) for p in props), default=0)
            metadata = {
                "partition_count": partition_count,
                "max_last_enqueued_sequence_number": last_seq,
                "eventhub_name": _self.eventhub_name,
                "namespace": _self.namespace,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_eventhubs_obs])
