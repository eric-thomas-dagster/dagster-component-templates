"""RabbitMQ Monitor Sensor Component.

Polls a RabbitMQ queue for new messages and triggers jobs when messages are received.
Passes message information via run_config to downstream assets. Messages are
acknowledged after RunRequests are created.
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


class RabbitMQMonitorSensorComponent(Component, Model, Resolvable):
    """Component for polling a RabbitMQ queue for new messages.

    This sensor uses basic_get to poll a RabbitMQ queue without holding a
    persistent consumer connection. Messages are acknowledged after RunRequests
    are created.

    Example:
        ```yaml
        type: dagster_component_templates.RabbitMQMonitorSensorComponent
        attributes:
          sensor_name: rabbitmq_events_sensor
          host: localhost
          queue_name: data-ingestion-queue
          job_name: process_rabbitmq_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    host: str = Field(description="RabbitMQ broker hostname or IP address")

    queue_name: str = Field(description="Name of the RabbitMQ queue to monitor")

    job_name: str = Field(description="Name of the job to trigger when messages are received")

    port: int = Field(default=5672, description="RabbitMQ AMQP port (default: 5672, TLS: 5671)")

    virtual_host: str = Field(default="/", description="RabbitMQ virtual host")

    username_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the RabbitMQ username"
    )

    password_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the RabbitMQ password"
    )

    use_tls: bool = Field(default=False, description="Whether to use TLS for the connection")

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to consume per evaluation"
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
        host = self.host
        queue_name = self.queue_name
        job_name = self.job_name
        port = self.port
        virtual_host = self.virtual_host
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
        use_tls = self.use_tls
        max_messages_per_poll = self.max_messages_per_poll
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
        def rabbitmq_sensor(context: SensorEvaluationContext):
            """Sensor that polls a RabbitMQ queue for new messages."""
            import os

            try:
                import pika
            except ImportError:
                return SensorResult(skip_reason="pika is not installed. Run: pip install pika")

            # Resolve credentials
            username = os.environ.get(username_env_var, "guest") if username_env_var else "guest"
            password = os.environ.get(password_env_var, "guest") if password_env_var else "guest"

            credentials = pika.PlainCredentials(username, password)
            params = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=credentials,
                ssl_options=pika.SSLOptions() if use_tls else None,
                connection_attempts=2,
                retry_delay=1,
            )

            run_requests = []
            channel = None
            connection = None

            try:
                connection = pika.BlockingConnection(params)
                channel = connection.channel()

                for _ in range(max_messages_per_poll):
                    method_frame, properties, body = channel.basic_get(
                        queue=queue_name, auto_ack=False
                    )
                    if method_frame is None:
                        break  # Queue is empty

                    delivery_tag = method_frame.delivery_tag
                    routing_key = method_frame.routing_key
                    exchange = method_frame.exchange
                    message_id = getattr(properties, "message_id", "") or ""
                    content_type = getattr(properties, "content_type", "") or ""
                    body_str = body.decode("utf-8", errors="replace") if body else ""

                    run_key = message_id if message_id else f"{queue_name}-{delivery_tag}"
                    run_requests.append(
                        RunRequest(
                            run_key=run_key,
                            run_config={
                                "ops": {
                                    "config": {
                                        "queue": queue_name,
                                        "routing_key": routing_key,
                                        "exchange": exchange,
                                        "message_id": message_id,
                                        "body": body_str,
                                        "content_type": content_type,
                                    }
                                }
                            },
                        )
                    )

                    channel.basic_ack(delivery_tag=delivery_tag)

            except Exception as e:
                context.log.error(f"Error polling RabbitMQ queue '{queue_name}' on {host}: {e}")
                return SensorResult(skip_reason=f"Error polling RabbitMQ: {e}")
            finally:
                if connection and not connection.is_closed:
                    connection.close()

            if run_requests:
                context.log.info(f"Received {len(run_requests)} message(s) from RabbitMQ queue '{queue_name}'")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No messages in RabbitMQ queue '{queue_name}'")

        return Definitions(sensors=[rabbitmq_sensor])
