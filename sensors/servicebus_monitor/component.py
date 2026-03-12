"""Azure Service Bus Monitor Sensor Component.

Polls an Azure Service Bus queue or topic subscription for new messages and
triggers jobs when messages are received. Passes message information via run_config
to downstream assets. Messages are completed (deleted) after RunRequests are created.
"""

import os
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


class ServiceBusMonitorSensorComponent(Component, Model, Resolvable):
    """Component for polling an Azure Service Bus queue or topic subscription.

    This sensor receives messages from a Service Bus queue or topic subscription
    and triggers jobs when messages are received. Messages are completed (deleted
    from the queue) after RunRequests are created.

    Authentication uses DefaultAzureCredential by default. Alternatively, provide
    a connection string via an environment variable.

    Example:
        ```yaml
        type: dagster_component_templates.ServiceBusMonitorSensorComponent
        attributes:
          sensor_name: servicebus_sensor
          fully_qualified_namespace: mynamespace.servicebus.windows.net
          queue_name: my-queue
          job_name: process_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    job_name: str = Field(description="Name of the job to trigger when messages are received")

    fully_qualified_namespace: Optional[str] = Field(
        default=None,
        description="Service Bus namespace hostname (e.g., 'mynamespace.servicebus.windows.net'). "
                    "Required when not using a connection string."
    )

    connection_string_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the Service Bus connection string. "
                    "If not provided, DefaultAzureCredential is used with fully_qualified_namespace."
    )

    queue_name: Optional[str] = Field(
        default=None,
        description="Name of the Service Bus queue to monitor. Provide this OR topic_name."
    )

    topic_name: Optional[str] = Field(
        default=None,
        description="Name of the Service Bus topic to monitor. Requires subscription_name."
    )

    subscription_name: Optional[str] = Field(
        default=None,
        description="Name of the topic subscription (required when using topic_name)"
    )

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to receive per evaluation"
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
        job_name = self.job_name
        fully_qualified_namespace = self.fully_qualified_namespace
        connection_string_env_var = self.connection_string_env_var
        queue_name = self.queue_name
        topic_name = self.topic_name
        subscription_name = self.subscription_name
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
        def servicebus_sensor(context: SensorEvaluationContext):
            """Sensor that polls an Azure Service Bus queue or topic subscription."""
            try:
                from azure.servicebus import ServiceBusClient
            except ImportError:
                return SensorResult(
                    skip_reason="azure-servicebus is not installed. "
                                "Run: pip install azure-servicebus azure-identity"
                )

            # Validate config
            if not queue_name and not (topic_name and subscription_name):
                return SensorResult(
                    skip_reason="Provide either queue_name, or both topic_name and subscription_name"
                )

            # Build client
            try:
                if connection_string_env_var:
                    conn_str = os.environ.get(connection_string_env_var)
                    if not conn_str:
                        return SensorResult(
                            skip_reason=f"Environment variable '{connection_string_env_var}' is not set"
                        )
                    sb_client = ServiceBusClient.from_connection_string(conn_str)
                else:
                    if not fully_qualified_namespace:
                        return SensorResult(
                            skip_reason="Either connection_string_env_var or fully_qualified_namespace must be provided"
                        )
                    from azure.identity import DefaultAzureCredential
                    sb_client = ServiceBusClient(
                        fully_qualified_namespace=fully_qualified_namespace,
                        credential=DefaultAzureCredential(),
                    )
            except Exception as e:
                context.log.error(f"Failed to create Service Bus client: {e}")
                return SensorResult(skip_reason=f"Failed to create Service Bus client: {e}")

            run_requests = []
            source_name = queue_name or f"{topic_name}/{subscription_name}"

            try:
                with sb_client:
                    if queue_name:
                        receiver = sb_client.get_queue_receiver(queue_name=queue_name)
                    else:
                        receiver = sb_client.get_subscription_receiver(
                            topic_name=topic_name,
                            subscription_name=subscription_name,
                        )

                    with receiver:
                        messages = receiver.receive_messages(
                            max_message_count=max_messages_per_poll,
                            max_wait_time=5,
                        )

                        for msg in messages:
                            message_id = msg.message_id or ""
                            body = "".join(
                                chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
                                for chunk in msg.body
                            )
                            subject = msg.subject or ""
                            content_type = msg.content_type or ""
                            enqueued_time = msg.enqueued_time_utc

                            run_requests.append(
                                RunRequest(
                                    run_key=str(message_id) if message_id else f"{source_name}-{id(msg)}",
                                    run_config={
                                        "ops": {
                                            "config": {
                                                "message_id": str(message_id),
                                                "body": body,
                                                "subject": subject,
                                                "content_type": content_type,
                                                "enqueued_time": enqueued_time.isoformat() if enqueued_time else "",
                                                "source": source_name,
                                            }
                                        }
                                    },
                                )
                            )

                            # Complete (delete) message
                            receiver.complete_message(msg)

            except Exception as e:
                context.log.error(f"Error receiving messages from Service Bus '{source_name}': {e}")
                return SensorResult(skip_reason=f"Error receiving Service Bus messages: {e}")

            if run_requests:
                context.log.info(f"Received {len(run_requests)} message(s) from Service Bus '{source_name}'")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No messages in Service Bus '{source_name}'")

        return Definitions(sensors=[servicebus_sensor])
