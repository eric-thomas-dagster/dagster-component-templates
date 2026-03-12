"""S3 Monitor Sensor Component.

Monitors an Amazon S3 bucket prefix for new objects and triggers jobs when objects are detected.
Passes object information via run_config to downstream assets.
"""

import re
from datetime import datetime, timezone
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


class S3MonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring an S3 bucket prefix for new objects.

    This sensor monitors an S3 bucket prefix for new objects and triggers
    jobs when objects are detected. Object information is passed to downstream
    assets via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.S3MonitorSensorComponent
        attributes:
          sensor_name: s3_data_sensor
          bucket_name: my-data-bucket
          prefix: incoming/
          key_pattern: ".*\\.parquet$"
          job_name: process_s3_objects_job
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor"
    )

    bucket_name: str = Field(
        description="Name of the S3 bucket to monitor"
    )

    prefix: str = Field(
        default="",
        description="S3 key prefix to scope monitoring (e.g., 'incoming/' or 'data/2024/')"
    )

    key_pattern: str = Field(
        default=".*",
        description="Regex pattern to match object keys (e.g., '.*\\.parquet$' for Parquet files)"
    )

    job_name: str = Field(
        description="Name of the job to trigger when objects are detected"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    region_name: Optional[str] = Field(
        default=None,
        description="AWS region name (e.g., 'us-east-1'). Uses default boto3 region if not set."
    )

    resource_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster resource key providing an AWS client. When set, "
                    "context.resources.<resource_key>.get_client() is used instead of creating "
                    "a boto3 client from the other fields. Compatible with dagster-aws S3Resource."
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        bucket_name = self.bucket_name
        prefix = self.prefix
        key_pattern = self.key_pattern
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        region_name = self.region_name
        resource_key = self.resource_key
        default_status_str = self.default_status

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
        def s3_sensor(context: SensorEvaluationContext):
            """Sensor that monitors an S3 bucket prefix for new objects."""
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 is not installed. Run: pip install boto3")

            # Get cursor (last processed object timestamp as ISO string)
            cursor = context.cursor or "1970-01-01T00:00:00+00:00"
            last_processed_time = datetime.fromisoformat(cursor)
            if last_processed_time.tzinfo is None:
                last_processed_time = last_processed_time.replace(tzinfo=timezone.utc)

            # Compile key pattern
            try:
                pattern = re.compile(key_pattern)
            except re.error as e:
                context.log.error(f"Invalid regex pattern: {key_pattern}. Error: {e}")
                return SensorResult(skip_reason=f"Invalid regex pattern: {key_pattern}")

            # Connect to S3 — via resource or inline
            try:
                if resource_key:
                    s3_client = getattr(context.resources, resource_key).get_client()
                else:
                    s3_client = boto3.client("s3", region_name=region_name)
            except Exception as e:
                context.log.error(f"Failed to create S3 client: {e}")
                return SensorResult(skip_reason=f"Failed to create S3 client: {e}")

            # List and process objects
            run_requests = []
            latest_time = last_processed_time

            try:
                paginator = s3_client.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

                for page in pages:
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        last_modified = obj["LastModified"]
                        size = obj["Size"]
                        etag = obj.get("ETag", "").strip('"')

                        # Skip if already processed
                        if last_modified <= last_processed_time:
                            continue

                        # Check pattern against key (relative to prefix)
                        relative_key = key[len(prefix):] if key.startswith(prefix) else key
                        if not pattern.search(relative_key):
                            continue

                        run_requests.append(
                            RunRequest(
                                run_key=f"{bucket_name}/{key}-{etag}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "bucket": bucket_name,
                                            "key": key,
                                            "size": size,
                                            "etag": etag,
                                            "last_modified": last_modified.isoformat(),
                                            "prefix": prefix,
                                            "region": region_name or "",
                                        }
                                    }
                                },
                            )
                        )

                        latest_time = max(latest_time, last_modified)

            except Exception as e:
                context.log.error(f"Error listing S3 objects in {bucket_name}/{prefix}: {e}")
                return SensorResult(skip_reason=f"Error listing S3 objects: {e}")

            if run_requests:
                context.log.info(f"Found {len(run_requests)} new S3 object(s) in {bucket_name}/{prefix}")
                return SensorResult(
                    run_requests=run_requests,
                    cursor=latest_time.isoformat(),
                )

            return SensorResult(skip_reason="No new S3 objects found")

        return Definitions(sensors=[s3_sensor])
