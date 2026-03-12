"""Apache Pulsar Monitor Sensor Component.

Monitors an Apache Pulsar topic for new messages and triggers jobs when messages
are detected. Passes message information via run_config to downstream assets.
Messages are acknowledged after RunRequests are created.
"""

from typing import Optional

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class PulsarMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring an Apache Pulsar topic for new messages.

    This sensor creates a durable subscription on a Pulsar topic, receives
    messages up to a configured limit, and triggers jobs when messages are
    detected. Messages are acknowledged after RunRequests are created.

    Example:
        ```yaml
        type: dagster_component_templates.PulsarMonitorSensorComponent
        attributes:
          sensor_name: pulsar_events_sensor
          service_url: pulsar://localhost:6650
          topic: persistent://public/default/events
          subscription_name: dagster-sensor-sub
          job_name: process_pulsar_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    service_url: str = Field(
        description="Pulsar service URL (e.g., 'pulsar://localhost:6650' or 'pulsar+ssl://host:6651')"
    )

    topic: str = Field(
        description="Pulsar topic name (e.g., 'persistent://public/default/my-topic')"
    )

    subscription_name: str = Field(
        description="Durable subscription name. Pulsar tracks the position per subscription."
    )

    job_name: str = Field(description="Name of the job to trigger when messages are detected")

    subscription_type: str = Field(
        default="Shared",
        description="Subscription type: Exclusive, Shared, Failover, or Key_Shared"
    )

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to consume per evaluation"
    )

    receive_timeout_ms: int = Field(
        default=5000,
        description="Milliseconds to wait for each message before stopping the poll"
    )

    jwt_token_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing a JWT auth token"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    resource_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster resource key providing a pre-configured client. "
                    "When set, context.resources.<resource_key> is used instead of creating "
                    "a connection from the other fields. See README for the expected interface."
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        service_url = self.service_url
        topic = self.topic
        subscription_name = self.subscription_name
        job_name = self.job_name
        subscription_type = self.subscription_type
        max_messages_per_poll = self.max_messages_per_poll
        receive_timeout_ms = self.receive_timeout_ms
        jwt_token_env_var = self.jwt_token_env_var
        minimum_interval_seconds = self.minimum_interval_seconds
        default_status_str = self.default_status
        resource_key = self.resource_key

        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys=required_resource_keys,
        )
        def pulsar_sensor(context: SensorEvaluationContext):
            """Sensor that monitors an Apache Pulsar topic for new messages."""
            import os

            try:
                import pulsar
            except ImportError:
                return SensorResult(
                    skip_reason="pulsar-client is not installed. Run: pip install pulsar-client"
                )

            # Build auth if JWT token is provided
            auth = None
            if jwt_token_env_var:
                token = os.environ.get(jwt_token_env_var)
                if not token:
                    return SensorResult(
                        skip_reason=f"Environment variable '{jwt_token_env_var}' is not set"
                    )
                auth = pulsar.AuthenticationToken(token)

            sub_type_map = {
                "Exclusive": pulsar.ConsumerType.Exclusive,
                "Shared": pulsar.ConsumerType.Shared,
                "Failover": pulsar.ConsumerType.Failover,
                "Key_Shared": pulsar.ConsumerType.KeyShared,
            }
            consumer_type = sub_type_map.get(subscription_type, pulsar.ConsumerType.Shared)

            client = None
            consumer = None
            run_requests = []

            try:
                client = pulsar.Client(service_url, authentication=auth)
                consumer = client.subscribe(
                    topic=topic,
                    subscription_name=subscription_name,
                    consumer_type=consumer_type,
                )

                for _ in range(max_messages_per_poll):
                    try:
                        msg = consumer.receive(timeout_millis=receive_timeout_ms)
                    except Exception:
                        break  # Timeout — no more messages

                    message_id = str(msg.message_id())
                    data = msg.data().decode("utf-8", errors="replace")
                    publish_time = msg.publish_timestamp()
                    event_time = msg.event_timestamp()
                    partition_key = msg.partition_key() or ""
                    properties = dict(msg.properties())

                    run_requests.append(
                        RunRequest(
                            run_key=f"{topic}-{message_id}",
                            run_config={
                                "ops": {
                                    "config": {
                                        "topic": topic,
                                        "message_id": message_id,
                                        "data": data,
                                        "publish_timestamp_ms": publish_time,
                                        "event_timestamp_ms": event_time,
                                        "partition_key": partition_key,
                                        "properties": str(properties),
                                        "subscription": subscription_name,
                                    }
                                }
                            },
                        )
                    )

                    consumer.acknowledge(msg)

            except Exception as e:
                context.log.error(f"Error reading from Pulsar topic '{topic}': {e}")
                return SensorResult(skip_reason=f"Error reading from Pulsar: {e}")
            finally:
                if consumer:
                    consumer.close()
                if client:
                    client.close()

            if run_requests:
                context.log.info(f"Received {len(run_requests)} message(s) from Pulsar topic '{topic}'")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No new messages on Pulsar topic '{topic}'")

        return Definitions(sensors=[pulsar_sensor])
