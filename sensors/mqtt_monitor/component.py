"""MQTT Monitor Sensor Component.

Subscribes to an MQTT broker topic, collects messages for a brief window, and
triggers jobs when messages are received. Passes message information via run_config
to downstream assets.
"""

import json
import threading
import time
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


class MQTTMonitorSensorComponent(Component, Model, Resolvable):
    """Component for subscribing to an MQTT broker topic for new messages.

    This sensor connects to an MQTT broker, subscribes to a topic, collects
    messages for a configurable window, then disconnects. Message content is
    passed to downstream assets via run_config.

    Ideal for IoT sensor data, telemetry pipelines, and device event streams.

    Example:
        ```yaml
        type: dagster_component_templates.MQTTMonitorSensorComponent
        attributes:
          sensor_name: mqtt_telemetry_sensor
          broker_host: mqtt.example.com
          topic: sensors/factory/+/temperature
          job_name: process_telemetry_job
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    broker_host: str = Field(description="MQTT broker hostname or IP address")

    topic: str = Field(
        description="MQTT topic to subscribe to. Supports wildcards: + (single level), # (multi-level)"
    )

    job_name: str = Field(description="Name of the job to trigger when messages are received")

    broker_port: int = Field(
        default=1883,
        description="MQTT broker port (default: 1883, TLS: 8883)"
    )

    qos: int = Field(
        default=1,
        description="MQTT Quality of Service level (0=at most once, 1=at least once, 2=exactly once)"
    )

    collect_window_seconds: float = Field(
        default=5.0,
        description="Seconds to listen for messages before disconnecting each evaluation"
    )

    max_messages_per_poll: int = Field(
        default=100,
        description="Maximum number of messages to collect per evaluation"
    )

    client_id: Optional[str] = Field(
        default=None,
        description="MQTT client ID. Auto-generated if not set."
    )

    use_tls: bool = Field(default=False, description="Whether to use TLS for the connection")

    username_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the MQTT username"
    )

    password_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the MQTT password"
    )

    minimum_interval_seconds: int = Field(
        default=60,
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
        broker_host = self.broker_host
        topic = self.topic
        job_name = self.job_name
        broker_port = self.broker_port
        qos = self.qos
        collect_window_seconds = self.collect_window_seconds
        max_messages_per_poll = self.max_messages_per_poll
        client_id = self.client_id
        use_tls = self.use_tls
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
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
        def mqtt_sensor(context: SensorEvaluationContext):
            """Sensor that subscribes to an MQTT topic and collects messages."""
            import os
            import uuid

            try:
                import paho.mqtt.client as mqtt
            except ImportError:
                return SensorResult(
                    skip_reason="paho-mqtt is not installed. Run: pip install paho-mqtt"
                )

            collected = []
            done_event = threading.Event()

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    client.subscribe(topic, qos=qos)
                else:
                    context.log.error(f"MQTT connection failed with code {rc}")
                    done_event.set()

            def on_message(client, userdata, msg):
                payload = msg.payload.decode("utf-8", errors="replace")
                collected.append({
                    "topic": msg.topic,
                    "payload": payload,
                    "qos": msg.qos,
                    "retain": msg.retain,
                    "timestamp_s": time.time(),
                })
                if len(collected) >= max_messages_per_poll:
                    done_event.set()

            cid = client_id or f"dagster-sensor-{uuid.uuid4().hex[:8]}"
            client = mqtt.Client(client_id=cid)

            if use_tls:
                client.tls_set()

            if username_env_var:
                username = os.environ.get(username_env_var, "")
                password = os.environ.get(password_env_var, "") if password_env_var else ""
                client.username_pw_set(username, password)

            client.on_connect = on_connect
            client.on_message = on_message

            try:
                client.connect(broker_host, broker_port, keepalive=60)
                client.loop_start()
                done_event.wait(timeout=collect_window_seconds)
                client.loop_stop()
                client.disconnect()
            except Exception as e:
                context.log.error(f"Error connecting to MQTT broker {broker_host}:{broker_port}: {e}")
                return SensorResult(skip_reason=f"Error connecting to MQTT broker: {e}")

            run_requests = []
            for i, msg_data in enumerate(collected):
                # Use topic + timestamp as run_key for deduplication
                run_key = f"{msg_data['topic']}-{msg_data['timestamp_s']}-{i}"
                run_requests.append(
                    RunRequest(
                        run_key=run_key,
                        run_config={
                            "ops": {
                                "config": {
                                    "topic": msg_data["topic"],
                                    "payload": msg_data["payload"],
                                    "qos": msg_data["qos"],
                                    "retain": msg_data["retain"],
                                    "timestamp_s": msg_data["timestamp_s"],
                                    "broker_host": broker_host,
                                }
                            }
                        },
                    )
                )

            if run_requests:
                context.log.info(f"Collected {len(run_requests)} MQTT message(s) from topic '{topic}'")
                return SensorResult(run_requests=run_requests)

            return SensorResult(skip_reason=f"No MQTT messages received on topic '{topic}'")

        return Definitions(sensors=[mqtt_sensor])
