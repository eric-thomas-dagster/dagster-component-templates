"""RabbitMQ Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
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
        def _rabbit_obs(context: SensorEvaluationContext):
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
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_rabbit_obs])
