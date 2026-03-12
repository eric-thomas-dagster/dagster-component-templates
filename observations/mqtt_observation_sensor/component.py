"""MQTT Observation Sensor Component."""
import threading
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
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
        def _mqtt_obs(context: SensorEvaluationContext):
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
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_mqtt_obs])
