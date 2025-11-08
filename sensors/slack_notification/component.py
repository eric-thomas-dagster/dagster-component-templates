"""Slack Notification Sensor Component."""

from typing import Optional
import os
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dagster import (
    Component,
    Resolvable,
    Definitions,
    SensorDefinition,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    ComponentLoadContext,
    DefaultSensorStatus,
)
from pydantic import BaseModel, Field


class SlackNotificationSensorComponent(Component, Resolvable, BaseModel):
    """
    Monitor Slack channels for new messages and trigger jobs.

    This sensor polls a Slack channel for new messages and triggers a job
    when messages match specified criteria. It uses cursor-based tracking
    to avoid reprocessing messages.
    """

    sensor_name: str = Field(description="Name of the sensor")
    slack_token: str = Field(description="Slack Bot token (use ${SLACK_TOKEN} for env var)")
    channel_id: str = Field(description="Slack channel ID to monitor")
    job_name: str = Field(description="Name of the job to trigger")
    keyword_filter: Optional[str] = Field(
        default="",
        description="Only trigger on messages containing this keyword"
    )
    user_filter: Optional[str] = Field(
        default="",
        description="Only trigger on messages from this user ID"
    )
    minimum_interval_seconds: int = Field(
        default=60,
        description="How often to check for new messages"
    )
    default_status: str = Field(
        default="RUNNING",
        description="Default sensor status (RUNNING or STOPPED)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the Slack sensor."""

        # Capture fields for closure
        slack_token = os.getenv("SLACK_TOKEN", self.slack_token)
        channel_id = self.channel_id
        job_name = self.job_name
        keyword_filter = self.keyword_filter
        user_filter = self.user_filter

        def sensor_fn(sensor_context: SensorEvaluationContext):
            """Sensor evaluation function."""
            try:
                # Initialize Slack client
                client = WebClient(token=slack_token)

                # Get last processed timestamp from cursor
                last_timestamp = float(sensor_context.cursor or "0")

                # Fetch recent messages
                response = client.conversations_history(
                    channel=channel_id,
                    oldest=str(last_timestamp),
                    limit=100
                )

                messages = response.get("messages", [])

                if not messages:
                    return SkipReason(f"No new messages in channel {channel_id}")

                # Filter messages by criteria
                filtered_messages = []
                for msg in messages:
                    # Skip if message is from a bot
                    if msg.get("bot_id"):
                        continue

                    # Apply keyword filter
                    if keyword_filter and keyword_filter.lower() not in msg.get("text", "").lower():
                        continue

                    # Apply user filter
                    if user_filter and msg.get("user") != user_filter:
                        continue

                    filtered_messages.append(msg)

                if not filtered_messages:
                    # Update cursor even if no matches
                    if messages:
                        latest_timestamp = max(float(msg.get("ts", "0")) for msg in messages)
                        sensor_context.update_cursor(str(latest_timestamp))
                    return SkipReason(f"No messages matching filters in channel {channel_id}")

                # Sort by timestamp
                filtered_messages.sort(key=lambda m: float(m.get("ts", "0")))

                # Update cursor to latest message
                latest_timestamp = float(filtered_messages[-1].get("ts", "0"))
                sensor_context.update_cursor(str(latest_timestamp))

                # Create run requests for each matching message
                for msg in filtered_messages:
                    yield RunRequest(
                        run_key=f"slack-{channel_id}-{msg['ts']}",
                        run_config={
                            "ops": {
                                "config": {
                                    "message_text": msg.get("text", ""),
                                    "user_id": msg.get("user", ""),
                                    "channel_id": channel_id,
                                    "timestamp": msg.get("ts", ""),
                                    "thread_ts": msg.get("thread_ts"),
                                }
                            }
                        },
                        tags={
                            "slack_channel": channel_id,
                            "slack_user": msg.get("user", "unknown"),
                            "source": "slack_sensor",
                        }
                    )

                sensor_context.log.info(
                    f"Triggered {len(filtered_messages)} runs for new Slack messages"
                )

            except SlackApiError as e:
                sensor_context.log.error(f"Slack API error: {e.response['error']}")
                return SkipReason(f"Slack API error: {e.response['error']}")
            except Exception as e:
                sensor_context.log.error(f"Error in Slack sensor: {str(e)}")
                return SkipReason(f"Error: {str(e)}")

        # Create the sensor
        sensor = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            job_name=job_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=(
                DefaultSensorStatus.RUNNING
                if self.default_status == "RUNNING"
                else DefaultSensorStatus.STOPPED
            ),
        )

        return Definitions(sensors=[sensor])
