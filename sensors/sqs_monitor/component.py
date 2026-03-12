"""SQS Monitor Sensor Component.

Polls an AWS SQS queue for new messages and triggers jobs when messages are received.
Passes message information via run_config to downstream assets. Messages are deleted
from the queue after RunRequests are created.
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


class SQSMonitorSensorComponent(Component, Model, Resolvable):
    """Component for polling an AWS SQS queue for new messages.

    This sensor polls an SQS queue for new messages and triggers jobs when
    messages are received. Message content is passed to downstream assets via
    run_config. Messages are deleted from the queue after RunRequests are created.

    Example:
        ```yaml
        type: dagster_component_templates.SQSMonitorSensorComponent
        attributes:
          sensor_name: sqs_events_sensor
          queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
          job_name: process_sqs_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    queue_url: str = Field(
        description="Full URL of the SQS queue to poll "
                    "(e.g., 'https://sqs.us-east-1.amazonaws.com/123456789/my-queue')"
    )

    job_name: str = Field(description="Name of the job to trigger when messages are received")

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    max_messages_per_poll: int = Field(
        default=10,
        description="Maximum number of messages to receive per evaluation (1–10, SQS limit)"
    )

    visibility_timeout_seconds: int = Field(
        default=30,
        description="Seconds a message stays hidden from other consumers while being processed"
    )

    region_name: Optional[str] = Field(
        default=None,
        description="AWS region name. Uses default boto3 region if not set."
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
        queue_url = self.queue_url
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_messages_per_poll = min(max(self.max_messages_per_poll, 1), 10)
        visibility_timeout_seconds = self.visibility_timeout_seconds
        region_name = self.region_name
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
        def sqs_sensor(context: SensorEvaluationContext):
            """Sensor that polls an SQS queue for new messages."""
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 is not installed. Run: pip install boto3")

            try:
                sqs = boto3.client("sqs", region_name=region_name)
            except Exception as e:
                context.log.error(f"Failed to create SQS client: {e}")
                return SensorResult(skip_reason=f"Failed to create SQS client: {e}")

            run_requests = []
            receipts_to_delete = []

            try:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=max_messages_per_poll,
                    VisibilityTimeout=visibility_timeout_seconds,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                )
            except Exception as e:
                context.log.error(f"Error receiving SQS messages from {queue_url}: {e}")
                return SensorResult(skip_reason=f"Error receiving SQS messages: {e}")

            for msg in response.get("Messages", []):
                message_id = msg["MessageId"]
                receipt_handle = msg["ReceiptHandle"]
                body = msg.get("Body", "")
                attributes = msg.get("Attributes", {})
                sent_timestamp = attributes.get("SentTimestamp", "")

                run_requests.append(
                    RunRequest(
                        run_key=message_id,
                        run_config={
                            "ops": {
                                "config": {
                                    "message_id": message_id,
                                    "receipt_handle": receipt_handle,
                                    "body": body,
                                    "sent_timestamp": sent_timestamp,
                                    "queue_url": queue_url,
                                    "attributes": str(attributes),
                                }
                            }
                        },
                    )
                )
                receipts_to_delete.append(receipt_handle)

            # Delete messages after creating RunRequests
            if receipts_to_delete:
                try:
                    entries = [
                        {"Id": str(i), "ReceiptHandle": r}
                        for i, r in enumerate(receipts_to_delete)
                    ]
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
                except Exception as e:
                    context.log.warning(f"Failed to delete SQS messages after polling: {e}")

            if run_requests:
                context.log.info(f"Received {len(run_requests)} message(s) from SQS queue")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason="No messages in SQS queue")

        return Definitions(sensors=[sqs_sensor])
