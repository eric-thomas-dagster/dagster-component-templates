"""Kinesis Monitor Sensor Component.

Monitors an AWS Kinesis Data Stream for new records and triggers jobs when records
are detected. Passes record information via run_config to downstream assets.
Per-shard sequence numbers are tracked in the sensor cursor.
"""

import json
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


class KinesisMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring an AWS Kinesis Data Stream for new records.

    This sensor reads records from all shards of a Kinesis stream and triggers
    jobs when records are detected. Per-shard sequence numbers are stored in the
    sensor cursor to avoid reprocessing after restarts.

    Example:
        ```yaml
        type: dagster_component_templates.KinesisMonitorSensorComponent
        attributes:
          sensor_name: kinesis_events_sensor
          stream_name: my-event-stream
          job_name: process_kinesis_records_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    stream_name: str = Field(description="Name of the Kinesis Data Stream to monitor")

    job_name: str = Field(description="Name of the job to trigger when records are detected")

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    max_records_per_shard: int = Field(
        default=100,
        description="Maximum number of records to read per shard per evaluation"
    )

    initial_position: str = Field(
        default="LATEST",
        description="Where to start reading on first run: LATEST (new records only) or TRIM_HORIZON (all records)"
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
        stream_name = self.stream_name
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_records_per_shard = self.max_records_per_shard
        initial_position = self.initial_position
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
        def kinesis_sensor(context: SensorEvaluationContext):
            """Sensor that monitors a Kinesis stream for new records."""
            import base64

            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 is not installed. Run: pip install boto3")

            # cursor: {shard_id: last_sequence_number}
            cursor_data = {}
            if context.cursor:
                try:
                    cursor_data = json.loads(context.cursor)
                except json.JSONDecodeError:
                    context.log.warning("Could not parse cursor, starting fresh")

            try:
                client = boto3.client("kinesis", region_name=region_name)
            except Exception as e:
                context.log.error(f"Failed to create Kinesis client: {e}")
                return SensorResult(skip_reason=f"Failed to create Kinesis client: {e}")

            # List shards
            try:
                shards = []
                paginator = client.get_paginator("list_shards")
                for page in paginator.paginate(StreamName=stream_name):
                    shards.extend(page["Shards"])
            except Exception as e:
                context.log.error(f"Error listing shards for stream '{stream_name}': {e}")
                return SensorResult(skip_reason=f"Error listing Kinesis shards: {e}")

            run_requests = []
            new_cursor_data = dict(cursor_data)

            for shard in shards:
                shard_id = shard["ShardId"]

                try:
                    # Get a shard iterator
                    if shard_id in cursor_data:
                        iterator_response = client.get_shard_iterator(
                            StreamName=stream_name,
                            ShardId=shard_id,
                            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                            StartingSequenceNumber=cursor_data[shard_id],
                        )
                    else:
                        iterator_response = client.get_shard_iterator(
                            StreamName=stream_name,
                            ShardId=shard_id,
                            ShardIteratorType=initial_position,
                        )

                    shard_iterator = iterator_response["ShardIterator"]
                    records_response = client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=max_records_per_shard,
                    )

                    for record in records_response.get("Records", []):
                        sequence_number = record["SequenceNumber"]
                        partition_key = record["PartitionKey"]
                        data_bytes = record["Data"]
                        arrival_timestamp = record["ApproximateArrivalTimestamp"]

                        # Encode binary data as base64 for run_config transport
                        data_b64 = base64.b64encode(data_bytes).decode("utf-8")

                        run_requests.append(
                            RunRequest(
                                run_key=f"{stream_name}-{shard_id}-{sequence_number}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "stream_name": stream_name,
                                            "shard_id": shard_id,
                                            "sequence_number": sequence_number,
                                            "partition_key": partition_key,
                                            "data_b64": data_b64,
                                            "arrival_timestamp": arrival_timestamp.isoformat(),
                                            "region": region_name or "",
                                        }
                                    }
                                },
                            )
                        )

                        new_cursor_data[shard_id] = sequence_number

                except Exception as e:
                    context.log.error(f"Error reading shard {shard_id}: {e}")
                    continue

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new record(s) across {len(shards)} shard(s) "
                    f"in stream '{stream_name}'"
                )
                return SensorResult(
                    run_requests=run_requests,
                    cursor=json.dumps(new_cursor_data),
                )

            return SensorResult(
                skip_reason=f"No new records in Kinesis stream '{stream_name}'",
                cursor=json.dumps(new_cursor_data),
            )

        return Definitions(sensors=[kinesis_sensor])
