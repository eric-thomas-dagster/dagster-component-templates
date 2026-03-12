"""Redis Streams Monitor Sensor Component.

Monitors a Redis Stream for new entries and triggers jobs when entries are detected.
Passes entry information via run_config to downstream assets. The last-seen entry
ID is tracked in the sensor cursor.
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


class RedisStreamsMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring a Redis Stream for new entries.

    This sensor uses XREAD to poll a Redis Stream for entries newer than the
    last-seen entry ID. The cursor stores the last entry ID, enabling exactly-once
    processing across sensor evaluations.

    Example:
        ```yaml
        type: dagster_component_templates.RedisStreamsMonitorSensorComponent
        attributes:
          sensor_name: redis_stream_sensor
          stream_name: events:incoming
          job_name: process_stream_entries_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    stream_name: str = Field(description="Name of the Redis Stream to monitor (e.g., 'events:incoming')")

    job_name: str = Field(description="Name of the job to trigger when entries are detected")

    host: str = Field(default="localhost", description="Redis host")

    port: int = Field(default=6379, description="Redis port")

    db: int = Field(default=0, description="Redis database index")

    password_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the Redis password"
    )

    use_tls: bool = Field(default=False, description="Whether to use TLS for the Redis connection")

    max_entries_per_poll: int = Field(
        default=100,
        description="Maximum number of stream entries to read per evaluation"
    )

    start_from_beginning: bool = Field(
        default=False,
        description="On first run, read from the beginning of the stream. "
                    "If false, only reads entries that arrive after the sensor starts."
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
        stream_name = self.stream_name
        job_name = self.job_name
        host = self.host
        port = self.port
        db = self.db
        password_env_var = self.password_env_var
        use_tls = self.use_tls
        max_entries_per_poll = self.max_entries_per_poll
        start_from_beginning = self.start_from_beginning
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
        def redis_streams_sensor(context: SensorEvaluationContext):
            """Sensor that reads new entries from a Redis Stream."""
            import os

            try:
                import redis
            except ImportError:
                return SensorResult(skip_reason="redis is not installed. Run: pip install redis")

            # cursor stores the last-seen entry ID
            # "0-0" reads from beginning; "$" reads only new entries
            last_id = context.cursor or ("0-0" if start_from_beginning else "$")

            password = os.environ.get(password_env_var) if password_env_var else None

            try:
                r = redis.Redis(
                    host=host,
                    port=port,
                    db=db,
                    password=password,
                    ssl=use_tls,
                    decode_responses=True,
                )
                r.ping()
            except Exception as e:
                context.log.error(f"Failed to connect to Redis at {host}:{port}: {e}")
                return SensorResult(skip_reason=f"Failed to connect to Redis: {e}")

            run_requests = []
            new_last_id = last_id

            try:
                results = r.xread(
                    streams={stream_name: last_id},
                    count=max_entries_per_poll,
                    block=0,  # non-blocking
                )
            except Exception as e:
                context.log.error(f"Error reading Redis Stream '{stream_name}': {e}")
                return SensorResult(skip_reason=f"Error reading Redis Stream: {e}")

            if results:
                for _stream, entries in results:
                    for entry_id, fields in entries:
                        run_requests.append(
                            RunRequest(
                                run_key=f"{stream_name}-{entry_id}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "stream_name": stream_name,
                                            "entry_id": entry_id,
                                            "fields": str(fields),
                                            "host": host,
                                            "port": port,
                                        }
                                    }
                                },
                            )
                        )
                        new_last_id = entry_id

            if run_requests:
                context.log.info(f"Found {len(run_requests)} new entry(s) in Redis Stream '{stream_name}'")
                return SensorResult(run_requests=run_requests, cursor=new_last_id)

            return SensorResult(
                skip_reason=f"No new entries in Redis Stream '{stream_name}'",
                cursor=new_last_id,
            )

        return Definitions(sensors=[redis_streams_sensor])
