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

    partition_mode: str = Field(
        default="run_config",
        description=(
            "How the sensor surfaces detected S3 objects to downstream assets:\n"
            "  'run_config' (default) — yields RunRequest with full S3 metadata "
            "in run_config (bucket, key, size, etag, last_modified, prefix, region). "
            "Pair with `file_ingestion.from_run_config`.\n"
            "  'dynamic_partition' — registers each new S3 key as a dynamic "
            "partition via context.instance.add_dynamic_partitions(), then yields "
            "RunRequest(partition_key=key). Pair with `file_ingestion` where "
            "`partition_type: dynamic` and `from_run_config.uri_template` uses "
            "`{partition_key}`. Lets you track every processed file and re-run "
            "any specific one on demand from the UI.\n"
            "  'both' — does both: registers a dynamic partition AND embeds the "
            "S3 metadata in run_config. Most info but the asset's config class "
            "must accept those fields."
        ),
    )
    dynamic_partitions_name: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_mode is 'dynamic_partition' or 'both'. "
            "Must match the asset's `dynamic_partition_name:` field."
        ),
    )
    partition_key_template: str = Field(
        default="{key}",
        description=(
            "Template for the partition key registered per detected S3 object. "
            "Available fields: {bucket}, {key}, {prefix}. Default `{key}` makes "
            "the partition key equal to the S3 object key. Use "
            "`s3://{bucket}/{key}` if you want the full URI as the partition "
            "key (so `file_ingestion`'s uri_template can just be "
            "`{partition_key}` with no further interpolation)."
        ),
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
        partition_mode = (self.partition_mode or "run_config").lower()
        dynamic_partitions_name = self.dynamic_partitions_name
        partition_key_template = self.partition_key_template or "{key}"

        if partition_mode not in ("run_config", "dynamic_partition", "both"):
            raise ValueError(
                f"partition_mode must be 'run_config' | 'dynamic_partition' | "
                f"'both', got {partition_mode!r}"
            )
        if partition_mode in ("dynamic_partition", "both") and not dynamic_partitions_name:
            raise ValueError(
                f"partition_mode={partition_mode!r} requires dynamic_partitions_name "
                "to match the downstream asset's dynamic_partition_name field."
            )

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
            new_partition_keys: list = []
            latest_time = last_processed_time
            from dagster import AddDynamicPartitionsRequest  # local import — only needed for non-default modes

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

                        # Build the metadata block once — both modes use parts of it.
                        config_block = {
                            "bucket": bucket_name,
                            "key": key,
                            "size": size,
                            "etag": etag,
                            "last_modified": last_modified.isoformat(),
                            "prefix": prefix,
                            "region": region_name or "",
                        }

                        if partition_mode in ("dynamic_partition", "both"):
                            partition_key = partition_key_template.format(
                                bucket=bucket_name, key=key, prefix=prefix
                            )
                            new_partition_keys.append(partition_key)
                            run_requests.append(
                                RunRequest(
                                    run_key=f"{bucket_name}/{key}-{etag}",
                                    partition_key=partition_key,
                                    run_config=(
                                        {"ops": {"config": config_block}}
                                        if partition_mode == "both" else None
                                    ),
                                )
                            )
                        else:  # run_config (default — existing behavior)
                            run_requests.append(
                                RunRequest(
                                    run_key=f"{bucket_name}/{key}-{etag}",
                                    run_config={"ops": {"config": config_block}},
                                )
                            )

                        latest_time = max(latest_time, last_modified)

            except Exception as e:
                context.log.error(f"Error listing S3 objects in {bucket_name}/{prefix}: {e}")
                return SensorResult(skip_reason=f"Error listing S3 objects: {e}")

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new S3 object(s) in {bucket_name}/{prefix} "
                    f"(partition_mode={partition_mode})"
                )
                dynamic_partitions_requests = (
                    [AddDynamicPartitionsRequest(
                        partitions_def_name=dynamic_partitions_name or "",
                        partition_keys=new_partition_keys,
                    )]
                    if new_partition_keys and dynamic_partitions_name else []
                )
                return SensorResult(
                    run_requests=run_requests,
                    dynamic_partitions_requests=dynamic_partitions_requests,
                    cursor=latest_time.isoformat(),
                )

            return SensorResult(skip_reason="No new S3 objects found")

        return Definitions(sensors=[s3_sensor])
