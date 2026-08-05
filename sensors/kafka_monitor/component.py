"""Kafka Monitor Sensor Component.

Monitors a Kafka topic for new messages and triggers jobs when messages are detected.
Passes message information via run_config to downstream assets.
"""

import json
from typing import Optional

from dagster import (
    AssetKey,
    AssetObservation,
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


class KafkaMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring a Kafka topic for new messages.

    This sensor polls a Kafka topic for new messages and triggers jobs when
    messages are detected. Message content is passed to downstream assets via
    run_config. Offsets are tracked in the sensor cursor to avoid reprocessing.

    Example:
        ```yaml
        type: dagster_component_templates.KafkaMonitorSensorComponent
        attributes:
          sensor_name: kafka_events_sensor
          bootstrap_servers: localhost:9092
          topic: user-events
          group_id: dagster-sensor-group
          job_name: process_kafka_messages_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor"
    )

    bootstrap_servers: str = Field(
        description="Comma-separated list of Kafka broker addresses (e.g., 'broker1:9092,broker2:9092')"
    )

    topic: str = Field(
        description="Name of the Kafka topic to monitor"
    )

    group_id: str = Field(
        description="Kafka consumer group ID for this sensor"
    )

    job_name: str = Field(
        description="Name of the job to trigger when messages are detected"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to consume per sensor evaluation"
    )

    poll_timeout_seconds: float = Field(
        default=5.0,
        description="Seconds to wait for messages during each poll call"
    )

    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)"
    )

    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism when using SASL_PLAINTEXT or SASL_SSL (e.g., PLAIN, SCRAM-SHA-256)"
    )

    sasl_username_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the SASL username"
    )

    sasl_password_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the SASL password"
    )

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster resource key providing a pre-configured client. "
            "When set, context.resources.<resource_key>.poll(topic, last_id) is "
            "called instead of building a KafkaConsumer from the other fields. "
            "The resource must return a list of dicts, each shaped "
            "{id, ts, source, payload}. Use this for demo-mode seams (swap in a "
            "fixture-backed client) or shared-auth wrappers."
        ),
    )

    op_name: Optional[str] = Field(
        default="config",
        description=(
            "Op name to key the run_config under. Emitted as "
            "`{'ops': {op_name: {'config': {...}}}}`. Defaults to 'config' for "
            "backward compat, but if the target job's op is not named 'config' "
            "(e.g. an @asset or @multi_asset with a custom name), set this to "
            "the actual op name — otherwise Dagster rejects the run config with "
            "'Received unexpected config entry \"config\" at path root:ops'."
        ),
    )

    source_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional asset key (e.g. 'kafka_user_events') to receive an "
            "AssetObservation on every tick that yields new messages. Metadata "
            "includes topic, new_messages, last_event_id, last_event_ts — lets "
            "an observable-source asset in the graph show a live timeline of "
            "message arrivals in the Dagster UI."
        ),
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        bootstrap_servers = self.bootstrap_servers
        topic = self.topic
        group_id = self.group_id
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_messages_per_poll = self.max_messages_per_poll
        poll_timeout_seconds = self.poll_timeout_seconds
        security_protocol = self.security_protocol
        sasl_mechanism = self.sasl_mechanism
        sasl_username_env_var = self.sasl_username_env_var
        sasl_password_env_var = self.sasl_password_env_var
        default_status_str = self.default_status
        resource_key = self.resource_key
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key

        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        required_resource_keys = {resource_key} if resource_key else set()

        def _observation(new_messages: int, last_id: Optional[str], last_ts) -> list:
            if not source_asset_key or new_messages == 0:
                return []
            return [
                AssetObservation(
                    asset_key=AssetKey.from_user_string(source_asset_key),
                    metadata={
                        "topic": topic,
                        "new_messages": new_messages,
                        "last_event_id": last_id or "",
                        "last_event_ts": str(last_ts) if last_ts is not None else "",
                    },
                )
            ]

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys=required_resource_keys,
        )
        def kafka_sensor(context: SensorEvaluationContext):
            """Sensor that polls a Kafka topic for new messages."""
            import os

            # ── Resource-backed path (demo-mode seam or shared client) ──────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    events = client.poll(topic, context.cursor)
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.poll failed: {e}")
                    return SensorResult(skip_reason=f"resource poll failed: {e}")

                run_requests = []
                last_id = context.cursor
                last_ts = None
                for event in events or []:
                    eid = str(event.get("id"))
                    ets = event.get("ts")
                    src = event.get("source", topic)
                    payload = event.get("payload")
                    payload_str = payload if isinstance(payload, str) else json.dumps(payload)
                    # Try to derive partition/offset from id if formatted "partition-offset"
                    partition_val, offset_val = 0, 0
                    if "-" in eid:
                        p, _, o = eid.rpartition("-")
                        try:
                            partition_val = int(p)
                            offset_val = int(o)
                        except ValueError:
                            pass
                    run_requests.append(
                        RunRequest(
                            run_key=f"{topic}-{eid}",
                            run_config={
                                "ops": {
                                    op_name: {
                                        "config": {
                                            # Native kafka shape (backward compat)
                                            "topic": topic,
                                            "partition": partition_val,
                                            "offset": offset_val,
                                            "key": "",
                                            "value": payload_str,
                                            "timestamp_ms": ets if isinstance(ets, int) else 0,
                                            "bootstrap_servers": bootstrap_servers,
                                            # Resource-contract shape
                                            "id": eid,
                                            "ts": ets,
                                            "source": src,
                                            "payload": payload_str,
                                        }
                                    }
                                }
                            },
                        )
                    )
                    last_id = eid
                    last_ts = ets

                if run_requests:
                    context.log.info(
                        f"Found {len(run_requests)} new message(s) on topic '{topic}' (via resource '{resource_key}')"
                    )
                    return SensorResult(
                        run_requests=run_requests,
                        cursor=last_id,
                        asset_events=_observation(len(run_requests), last_id, last_ts),
                    )
                return SensorResult(skip_reason=f"No new messages on topic '{topic}' (via resource '{resource_key}')")

            # ── kafka-python native path ────────────────────────────────────
            try:
                from kafka import KafkaConsumer, TopicPartition
                from kafka.errors import KafkaError  # noqa: F401
            except ImportError:
                return SensorResult(
                    skip_reason="kafka-python is not installed. Run: pip install kafka-python"
                )

            # Build consumer config
            consumer_config = {
                "bootstrap_servers": bootstrap_servers,
                "group_id": group_id,
                "security_protocol": security_protocol,
                "enable_auto_commit": False,
                "auto_offset_reset": "earliest",
                "consumer_timeout_ms": int(poll_timeout_seconds * 1000),
            }

            if sasl_mechanism:
                consumer_config["sasl_mechanism"] = sasl_mechanism

                if sasl_username_env_var:
                    username = os.environ.get(sasl_username_env_var)
                    if not username:
                        return SensorResult(
                            skip_reason=f"Environment variable '{sasl_username_env_var}' is not set"
                        )
                    consumer_config["sasl_plain_username"] = username

                if sasl_password_env_var:
                    password = os.environ.get(sasl_password_env_var)
                    if not password:
                        return SensorResult(
                            skip_reason=f"Environment variable '{sasl_password_env_var}' is not set"
                        )
                    consumer_config["sasl_plain_password"] = password

            # Load offset cursor: {"partition_id": offset, ...}
            cursor_data = {}
            if context.cursor:
                try:
                    cursor_data = json.loads(context.cursor)
                except json.JSONDecodeError:
                    context.log.warning("Could not parse cursor, starting from stored offsets")

            run_requests = []
            new_cursor_data = dict(cursor_data)
            last_event_id: Optional[str] = None
            last_event_ts = None

            try:
                consumer = KafkaConsumer(**consumer_config)

                # Get partition assignment for the topic
                partitions = consumer.partitions_for_topic(topic)
                if partitions is None:
                    consumer.close()
                    return SensorResult(skip_reason=f"Topic '{topic}' not found or no partitions available")

                topic_partitions = [TopicPartition(topic, p) for p in partitions]
                consumer.assign(topic_partitions)

                # Seek to stored offsets (or beginning/end for new partitions)
                for tp in topic_partitions:
                    partition_key = str(tp.partition)
                    if partition_key in cursor_data:
                        # Resume from stored offset
                        consumer.seek(tp, cursor_data[partition_key])
                    else:
                        # New partition — start from earliest
                        consumer.seek_to_beginning(tp)

                # Poll messages up to the max
                messages_consumed = 0
                try:
                    for message in consumer:
                        if messages_consumed >= max_messages_per_poll:
                            break

                        partition_key = str(message.partition)
                        offset = message.offset
                        key = message.key.decode("utf-8") if message.key else ""
                        value = message.value.decode("utf-8") if message.value else ""
                        timestamp_ms = message.timestamp

                        run_requests.append(
                            RunRequest(
                                run_key=f"{topic}-{message.partition}-{offset}",
                                run_config={
                                    "ops": {
                                        op_name: {
                                            "config": {
                                                # Native kafka shape (backward compat)
                                                "topic": topic,
                                                "partition": message.partition,
                                                "offset": offset,
                                                "key": key,
                                                "value": value,
                                                "timestamp_ms": timestamp_ms,
                                                "bootstrap_servers": bootstrap_servers,
                                                # Resource-contract shape (parity w/ demo-mode)
                                                "id": f"{message.partition}-{offset}",
                                                "ts": timestamp_ms,
                                                "source": topic,
                                                "payload": value,
                                            }
                                        }
                                    }
                                },
                            )
                        )

                        # Track the next offset to consume (offset + 1)
                        new_cursor_data[partition_key] = offset + 1
                        messages_consumed += 1
                        last_event_id = f"{message.partition}-{offset}"
                        last_event_ts = timestamp_ms

                except StopIteration:
                    # consumer_timeout_ms reached — no more messages
                    pass

                consumer.close()

            except Exception as e:
                context.log.error(f"Error consuming from Kafka topic '{topic}': {e}")
                return SensorResult(skip_reason=f"Error consuming from Kafka: {e}")

            if run_requests:
                context.log.info(f"Found {len(run_requests)} new message(s) on topic '{topic}'")
                return SensorResult(
                    run_requests=run_requests,
                    cursor=json.dumps(new_cursor_data),
                    asset_events=_observation(len(run_requests), last_event_id, last_event_ts),
                )

            return SensorResult(skip_reason=f"No new messages on topic '{topic}'")

        return Definitions(sensors=[kafka_sensor])
