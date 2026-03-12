"""Google Pub/Sub Monitor Sensor Component.

Pulls messages from a Google Cloud Pub/Sub subscription and triggers jobs when
messages are received. Passes message information via run_config to downstream
assets. Messages are acknowledged after RunRequests are created.
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


class PubSubMonitorSensorComponent(Component, Model, Resolvable):
    """Component for pulling messages from a Google Cloud Pub/Sub subscription.

    This sensor pulls messages from a Pub/Sub subscription and triggers jobs when
    messages are received. Messages are acknowledged after RunRequests are created.

    Authentication uses Application Default Credentials (ADC): Workload Identity,
    GOOGLE_APPLICATION_CREDENTIALS, or gcloud CLI login.

    Example:
        ```yaml
        type: dagster_component_templates.PubSubMonitorSensorComponent
        attributes:
          sensor_name: pubsub_events_sensor
          project: my-gcp-project
          subscription: my-subscription
          job_name: process_pubsub_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    project: str = Field(description="GCP project ID")

    subscription: str = Field(
        description="Pub/Sub subscription name (short name, not full path)"
    )

    job_name: str = Field(description="Name of the job to trigger when messages are received")

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    max_messages_per_pull: int = Field(
        default=100,
        description="Maximum number of messages to pull per evaluation"
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
        project = self.project
        subscription = self.subscription
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_messages_per_pull = self.max_messages_per_pull
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
        def pubsub_sensor(context: SensorEvaluationContext):
            """Sensor that pulls messages from a Pub/Sub subscription."""
            try:
                from google.cloud import pubsub_v1
            except ImportError:
                return SensorResult(
                    skip_reason="google-cloud-pubsub is not installed. Run: pip install google-cloud-pubsub"
                )

            subscription_path = f"projects/{project}/subscriptions/{subscription}"

            try:
                subscriber = pubsub_v1.SubscriberClient()
            except Exception as e:
                context.log.error(f"Failed to create Pub/Sub client: {e}")
                return SensorResult(skip_reason=f"Failed to create Pub/Sub client: {e}")

            run_requests = []
            ack_ids = []

            try:
                response = subscriber.pull(
                    request={
                        "subscription": subscription_path,
                        "max_messages": max_messages_per_pull,
                    }
                )
            except Exception as e:
                context.log.error(f"Error pulling from Pub/Sub subscription '{subscription}': {e}")
                return SensorResult(skip_reason=f"Error pulling Pub/Sub messages: {e}")
            finally:
                subscriber.close()

            for received_message in response.received_messages:
                msg = received_message.message
                message_id = msg.message_id
                data = msg.data.decode("utf-8") if msg.data else ""
                attributes = dict(msg.attributes)
                publish_time = msg.publish_time

                run_requests.append(
                    RunRequest(
                        run_key=message_id,
                        run_config={
                            "ops": {
                                "config": {
                                    "message_id": message_id,
                                    "data": data,
                                    "attributes": str(attributes),
                                    "publish_time": publish_time.isoformat(),
                                    "subscription": subscription,
                                    "project": project,
                                }
                            }
                        },
                    )
                )
                ack_ids.append(received_message.ack_id)

            # Acknowledge messages after creating RunRequests
            if ack_ids:
                try:
                    subscriber = pubsub_v1.SubscriberClient()
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids}
                    )
                    subscriber.close()
                except Exception as e:
                    context.log.warning(f"Failed to acknowledge Pub/Sub messages: {e}")

            if run_requests:
                context.log.info(f"Received {len(run_requests)} message(s) from Pub/Sub subscription '{subscription}'")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No messages in Pub/Sub subscription '{subscription}'")

        return Definitions(sensors=[pubsub_sensor])
