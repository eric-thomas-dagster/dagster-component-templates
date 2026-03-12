"""NATS JetStream Monitor Sensor Component.

Monitors a NATS JetStream consumer for new messages and triggers jobs when
messages are detected. Passes message information via run_config to downstream
assets. Messages are acknowledged after RunRequests are created.
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


class NATSMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring a NATS JetStream consumer for new messages.

    This sensor fetches messages from a durable JetStream consumer and triggers
    jobs when messages are detected. Messages are acknowledged after RunRequests
    are created. JetStream tracks consumer position server-side.

    Example:
        ```yaml
        type: dagster_component_templates.NATSMonitorSensorComponent
        attributes:
          sensor_name: nats_events_sensor
          servers: nats://localhost:4222
          stream_name: EVENTS
          consumer_name: dagster-sensor
          job_name: process_nats_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    servers: str = Field(
        description="Comma-separated NATS server URLs (e.g., 'nats://localhost:4222')"
    )

    stream_name: str = Field(description="JetStream stream name")

    consumer_name: str = Field(
        description="Durable JetStream consumer name. Must be pre-created on the server."
    )

    job_name: str = Field(description="Name of the job to trigger when messages are detected")

    subject_filter: Optional[str] = Field(
        default=None,
        description="Optional subject filter to scope which messages are processed"
    )

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to fetch per evaluation"
    )

    fetch_timeout_seconds: float = Field(
        default=5.0,
        description="Seconds to wait for messages during each fetch call"
    )

    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the path to a NATS .creds file"
    )

    jwt_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing a NATS JWT token"
    )

    nkey_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing a NATS NKey seed"
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
        servers = self.servers
        stream_name = self.stream_name
        consumer_name = self.consumer_name
        job_name = self.job_name
        subject_filter = self.subject_filter
        max_messages_per_poll = self.max_messages_per_poll
        fetch_timeout_seconds = self.fetch_timeout_seconds
        credentials_env_var = self.credentials_env_var
        jwt_env_var = self.jwt_env_var
        nkey_env_var = self.nkey_env_var
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
        def nats_sensor(context: SensorEvaluationContext):
            """Sensor that fetches messages from a NATS JetStream consumer."""
            import asyncio

            try:
                import nats
            except ImportError:
                return SensorResult(
                    skip_reason="nats-py is not installed. Run: pip install nats-py"
                )

            server_list = [s.strip() for s in servers.split(",")]

            # Build connect kwargs
            connect_kwargs = {"servers": server_list}
            if credentials_env_var:
                creds_path = os.environ.get(credentials_env_var)
                if not creds_path:
                    return SensorResult(
                        skip_reason=f"Environment variable '{credentials_env_var}' is not set"
                    )
                connect_kwargs["credentials"] = creds_path
            elif jwt_env_var and nkey_env_var:
                connect_kwargs["user_credentials"] = (
                    os.environ.get(jwt_env_var, ""),
                    os.environ.get(nkey_env_var, ""),
                )

            run_requests = []

            async def fetch_messages():
                nc = await nats.connect(**connect_kwargs)
                js = nc.jetstream()

                try:
                    consumer = await js.consumer(stream_name, consumer_name)
                    msgs = await consumer.fetch(
                        batch=max_messages_per_poll,
                        timeout=fetch_timeout_seconds,
                    )

                    for msg in msgs:
                        subject = msg.subject
                        data = msg.data.decode("utf-8", errors="replace") if msg.data else ""
                        headers = dict(msg.headers) if msg.headers else {}
                        seq = msg.metadata.sequence.stream if msg.metadata else 0
                        timestamp_ns = int(msg.metadata.timestamp.timestamp() * 1e9) if msg.metadata else 0

                        if subject_filter and subject_filter not in subject:
                            await msg.ack()
                            continue

                        run_requests.append(
                            RunRequest(
                                run_key=f"{stream_name}-{seq}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "stream": stream_name,
                                            "consumer": consumer_name,
                                            "subject": subject,
                                            "sequence": seq,
                                            "data": data,
                                            "headers": str(headers),
                                            "timestamp_ns": timestamp_ns,
                                        }
                                    }
                                },
                            )
                        )
                        await msg.ack()

                finally:
                    await nc.drain()

            try:
                asyncio.run(fetch_messages())
            except Exception as e:
                context.log.error(f"Error fetching from NATS JetStream '{stream_name}': {e}")
                return SensorResult(skip_reason=f"Error fetching from NATS: {e}")

            if run_requests:
                context.log.info(
                    f"Received {len(run_requests)} message(s) from NATS stream '{stream_name}'"
                )
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No new messages in NATS stream '{stream_name}'")

        return Definitions(sensors=[nats_sensor])
