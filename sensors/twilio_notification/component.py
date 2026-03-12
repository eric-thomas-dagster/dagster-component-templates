"""Twilio Notification Sensor Component.

Sends SMS or WhatsApp messages when Dagster asset materializations fail or
succeed.  Uses the Twilio REST API directly via requests — no Twilio SDK
required.
"""

import os
from datetime import datetime, timezone
from typing import List, Optional

import requests

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from dagster._core.events import DagsterEventType
from pydantic import Field


class TwilioNotificationSensor(Component, Model, Resolvable):
    """Component that sends SMS or WhatsApp notifications when Dagster assets
    fail or succeed.

    Matches the notification-sensor pattern of ``slack_notification`` and
    ``pagerduty_alert``.  Uses the Twilio Messaging REST API directly so no
    Twilio Python SDK is required — only ``requests``.

    Example:
        ```yaml
        type: dagster_component_templates.TwilioNotificationSensor
        attributes:
          sensor_name: pipeline_sms_alerts
          account_sid_env_var: TWILIO_ACCOUNT_SID
          auth_token_env_var: TWILIO_AUTH_TOKEN
          from_number: "+15551234567"
          to_numbers:
            - "+15559876543"
          on_failure: true
          on_success: false
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor."
    )

    account_sid_env_var: str = Field(
        description=(
            "Name of the environment variable that holds the Twilio Account SID "
            "(e.g., TWILIO_ACCOUNT_SID)."
        )
    )

    auth_token_env_var: str = Field(
        description=(
            "Name of the environment variable that holds the Twilio Auth Token "
            "(e.g., TWILIO_AUTH_TOKEN)."
        )
    )

    from_number: str = Field(
        description=(
            "The Twilio phone number to send from (e.g., '+15551234567'). "
            "For WhatsApp use the plain E.164 number — the 'whatsapp:' prefix is "
            "added automatically when channel='whatsapp'."
        )
    )

    to_numbers: List[str] = Field(
        description="List of recipient phone numbers in E.164 format (e.g., '+15559876543')."
    )

    channel: str = Field(
        default="sms",
        description=(
            "Messaging channel to use. 'sms' sends a standard SMS; "
            "'whatsapp' prefixes both from/to numbers with 'whatsapp:' automatically."
        ),
    )

    message_template: str = Field(
        default="Dagster alert: {asset_key} {event_type} at {timestamp}",
        description=(
            "Template string for the notification message. "
            "Available format variables: {asset_key}, {event_type}, {timestamp}, {run_id}."
        ),
    )

    monitored_assets: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of asset key strings to watch (e.g., ['my_asset', 'namespace/other']). "
            "When None (default) all asset events are monitored."
        ),
    )

    on_failure: bool = Field(
        default=True,
        description="Send a notification when an asset materialization fails.",
    )

    on_success: bool = Field(
        default=False,
        description="Send a notification when an asset materialization succeeds.",
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Capture fields for the closure.
        sensor_name = self.sensor_name
        account_sid_env_var = self.account_sid_env_var
        auth_token_env_var = self.auth_token_env_var
        from_number = self.from_number
        to_numbers = list(self.to_numbers)
        channel = self.channel.lower()
        message_template = self.message_template
        monitored_assets = set(self.monitored_assets) if self.monitored_assets else None
        on_failure = self.on_failure
        on_success = self.on_success
        minimum_interval_seconds = self.minimum_interval_seconds

        def _twilio_from(number: str) -> str:
            return f"whatsapp:{number}" if channel == "whatsapp" else number

        def _twilio_to(number: str) -> str:
            return f"whatsapp:{number}" if channel == "whatsapp" else number

        def _send_sms(sid: str, token: str, body: str) -> None:
            """Send body to every recipient via the Twilio Messages API."""
            url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
            for to in to_numbers:
                try:
                    resp = requests.post(
                        url,
                        auth=(sid, token),
                        data={
                            "From": _twilio_from(from_number),
                            "To": _twilio_to(to),
                            "Body": body,
                        },
                        timeout=10,
                    )
                    resp.raise_for_status()
                except requests.RequestException as exc:
                    # Raise so the sensor can log and skip gracefully.
                    raise RuntimeError(
                        f"Twilio API error sending to {to}: {exc}"
                    ) from exc

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING,
        )
        def twilio_notification_sensor(context: SensorEvaluationContext):
            """Sensor that sends Twilio SMS/WhatsApp alerts on asset events."""
            # Resolve credentials at evaluation time.
            account_sid = os.environ.get(account_sid_env_var)
            auth_token = os.environ.get(auth_token_env_var)

            if not account_sid:
                return SensorResult(
                    skip_reason=(
                        f"Environment variable '{account_sid_env_var}' is not set."
                    )
                )
            if not auth_token:
                return SensorResult(
                    skip_reason=(
                        f"Environment variable '{auth_token_env_var}' is not set."
                    )
                )

            if not on_failure and not on_success:
                return SensorResult(
                    skip_reason="Both on_failure and on_success are False — nothing to monitor."
                )

            # Cursor stores the storage ID of the last event we processed.
            after_storage_id = int(context.cursor or "0")

            # Collect the event types we care about.
            event_types_to_watch = []
            if on_failure:
                event_types_to_watch.append(DagsterEventType.ASSET_MATERIALIZATION_PLANNED)
                # We catch step failures as a proxy for materialisation failures.
                event_types_to_watch.append(DagsterEventType.STEP_FAILURE)
            if on_success:
                event_types_to_watch.append(DagsterEventType.ASSET_MATERIALIZATION)

            try:
                # Query the instance event log for recent records.
                records = context.instance.get_event_records(
                    event_records_filter=None,
                    limit=200,
                    ascending=True,
                    after_storage_id=after_storage_id,
                )
            except Exception as exc:
                context.log.error(f"Failed to query event log: {exc}")
                return SensorResult(skip_reason=f"Event log query failed: {exc}")

            if not records:
                return SensorResult(skip_reason="No new events since last evaluation.")

            new_max_storage_id = after_storage_id
            notifications_sent = 0
            errors = []

            for record in records:
                new_max_storage_id = max(new_max_storage_id, record.storage_id)

                event = record.event_log_entry.dagster_event
                if event is None:
                    continue

                # Filter by the event types we care about.
                if event.event_type not in event_types_to_watch:
                    continue

                # Derive a human-readable asset key and event label.
                asset_key_str = "unknown"
                event_type_label = event.event_type.value

                if event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
                    mat = event.asset_materialization
                    if mat is not None:
                        asset_key_str = mat.asset_key.to_user_string()
                        event_type_label = "materialized successfully"
                elif event.event_type == DagsterEventType.STEP_FAILURE:
                    asset_key_str = event.step_key or "unknown"
                    event_type_label = "FAILED"
                elif event.event_type == DagsterEventType.ASSET_MATERIALIZATION_PLANNED:
                    if event.asset_key:
                        asset_key_str = event.asset_key.to_user_string()
                    event_type_label = "materialization planned"

                # Apply asset filter.
                if monitored_assets is not None and asset_key_str not in monitored_assets:
                    continue

                run_id = record.event_log_entry.run_id or "N/A"
                timestamp = datetime.fromtimestamp(
                    record.event_log_entry.timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")

                try:
                    body = message_template.format(
                        asset_key=asset_key_str,
                        event_type=event_type_label,
                        timestamp=timestamp,
                        run_id=run_id,
                    )
                except KeyError as exc:
                    body = (
                        f"Dagster alert: {asset_key_str} {event_type_label} "
                        f"at {timestamp} (run: {run_id})"
                    )
                    context.log.warning(
                        f"message_template contains unknown placeholder {exc}; "
                        "using fallback message."
                    )

                try:
                    _send_sms(account_sid, auth_token, body)
                    notifications_sent += 1
                    context.log.info(
                        f"Sent {channel.upper()} notification for {asset_key_str} "
                        f"({event_type_label}) to {len(to_numbers)} recipient(s)."
                    )
                except RuntimeError as exc:
                    errors.append(str(exc))
                    context.log.error(str(exc))

            # Always advance the cursor so we do not reprocess old events.
            skip_parts = []
            if notifications_sent == 0 and not errors:
                skip_parts.append("No matching asset events found.")
            if errors:
                skip_parts.append(f"{len(errors)} Twilio error(s): {'; '.join(errors)}")

            if notifications_sent > 0:
                context.log.info(
                    f"Sent {notifications_sent} notification(s) this evaluation."
                )

            return SensorResult(
                run_requests=[],
                cursor=str(new_max_storage_id),
                skip_reason="; ".join(skip_parts) if skip_parts else None,
            )

        return Definitions(sensors=[twilio_notification_sensor])
