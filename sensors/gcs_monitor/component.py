"""GCS Monitor Sensor Component.

Monitors a Google Cloud Storage bucket prefix for new objects and triggers
jobs when objects are detected. Passes object information via run_config to downstream assets.
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


class GCSMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring a Google Cloud Storage bucket prefix for new objects.

    This sensor monitors a GCS bucket prefix for new objects and triggers
    jobs when objects are detected. Object information is passed to downstream
    assets via run_config.

    Authentication uses Application Default Credentials (ADC) by default, which
    supports Workload Identity (GKE), service account key files
    (GOOGLE_APPLICATION_CREDENTIALS env var), and gcloud CLI login.

    Example:
        ```yaml
        type: dagster_component_templates.GCSMonitorSensorComponent
        attributes:
          sensor_name: gcs_data_sensor
          bucket_name: my-data-bucket
          prefix: incoming/
          blob_pattern: ".*\\.parquet$"
          job_name: process_gcs_objects_job
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor"
    )

    bucket_name: str = Field(
        description="Name of the GCS bucket to monitor"
    )

    prefix: str = Field(
        default="",
        description="Object prefix to scope monitoring (e.g., 'incoming/' or 'data/2024/')"
    )

    blob_pattern: str = Field(
        default=".*",
        description="Regex pattern to match object names relative to the prefix (e.g., '.*\\.parquet$')"
    )

    job_name: str = Field(
        description="Name of the job to trigger when objects are detected"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    project: Optional[str] = Field(
        default=None,
        description="GCP project ID. Uses the project from ADC if not set."
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

    partition_mode: str = Field(
        default="run_config",
        description=(
            "How the sensor surfaces detected GCS objects: 'run_config' "
            "(default, embeds metadata in run_config), 'dynamic_partition' "
            "(registers each new blob name as a dynamic partition + yields "
            "RunRequest(partition_key=...)), or 'both'. Pair with "
            "`file_ingestion`'s matching input mode."
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
        default="{name}",
        description=(
            "Template for the partition key per detected object. Fields: "
            "{bucket}, {name}, {prefix}. Default `{name}` makes the partition "
            "key equal to the blob name. Use `gs://{bucket}/{name}` for the "
            "full URI as the partition key."
        ),
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        bucket_name = self.bucket_name
        prefix = self.prefix
        blob_pattern = self.blob_pattern
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        project = self.project
        default_status_str = self.default_status
        resource_key = self.resource_key
        partition_mode = (self.partition_mode or "run_config").lower()
        dynamic_partitions_name = self.dynamic_partitions_name
        partition_key_template = self.partition_key_template or "{name}"
        if partition_mode not in ("run_config", "dynamic_partition", "both"):
            raise ValueError(
                f"partition_mode must be 'run_config' | 'dynamic_partition' | "
                f"'both', got {partition_mode!r}"
            )
        if partition_mode in ("dynamic_partition", "both") and not dynamic_partitions_name:
            raise ValueError(
                f"partition_mode={partition_mode!r} requires dynamic_partitions_name."
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
        def gcs_sensor(context: SensorEvaluationContext):
            """Sensor that monitors a GCS bucket prefix for new objects."""
            try:
                from google.cloud import storage as gcs
            except ImportError:
                return SensorResult(
                    skip_reason="google-cloud-storage is not installed. Run: pip install google-cloud-storage"
                )

            # Get cursor (last processed object timestamp as ISO string)
            cursor = context.cursor or "1970-01-01T00:00:00+00:00"
            last_processed_time = datetime.fromisoformat(cursor)
            if last_processed_time.tzinfo is None:
                last_processed_time = last_processed_time.replace(tzinfo=timezone.utc)

            # Compile blob pattern
            try:
                pattern = re.compile(blob_pattern)
            except re.error as e:
                context.log.error(f"Invalid regex pattern: {blob_pattern}. Error: {e}")
                return SensorResult(skip_reason=f"Invalid regex pattern: {blob_pattern}")

            # Connect to GCS
            try:
                client = gcs.Client(project=project)
                bucket = client.bucket(bucket_name)
            except Exception as e:
                context.log.error(f"Failed to create GCS client: {e}")
                return SensorResult(skip_reason=f"Failed to create GCS client: {e}")

            # List and process objects
            run_requests = []
            new_partition_keys: list = []
            latest_time = last_processed_time
            from dagster import AddDynamicPartitionsRequest  # local import

            try:
                blobs = client.list_blobs(bucket_name, prefix=prefix)

                for blob in blobs:
                    updated = blob.updated
                    if updated is None:
                        continue

                    # Normalize timezone
                    if updated.tzinfo is None:
                        updated = updated.replace(tzinfo=timezone.utc)

                    # Skip if already processed
                    if updated <= last_processed_time:
                        continue

                    # Match pattern against name relative to prefix
                    relative_name = blob.name[len(prefix):] if blob.name.startswith(prefix) else blob.name

                    # Skip "directory" placeholder blobs (zero-byte objects ending in /)
                    if blob.name.endswith("/"):
                        continue

                    if not pattern.search(relative_name):
                        continue

                    config_block = {
                        "bucket": bucket_name,
                        "name": blob.name,
                        "size": blob.size or 0,
                        "etag": blob.etag or "",
                        "updated": updated.isoformat(),
                        "content_type": blob.content_type or "",
                        "prefix": prefix,
                        "project": project or "",
                    }
                    if partition_mode in ("dynamic_partition", "both"):
                        partition_key = partition_key_template.format(
                            bucket=bucket_name, name=blob.name, prefix=prefix
                        )
                        new_partition_keys.append(partition_key)
                        run_requests.append(
                            RunRequest(
                                run_key=f"{bucket_name}/{blob.name}-{blob.etag}",
                                partition_key=partition_key,
                                run_config=(
                                    {"ops": {"config": config_block}}
                                    if partition_mode == "both" else None
                                ),
                            )
                        )
                    else:
                        run_requests.append(
                            RunRequest(
                                run_key=f"{bucket_name}/{blob.name}-{blob.etag}",
                                run_config={"ops": {"config": config_block}},
                            )
                        )
                    latest_time = max(latest_time, updated)

            except Exception as e:
                context.log.error(f"Error listing GCS objects in {bucket_name}/{prefix}: {e}")
                return SensorResult(skip_reason=f"Error listing GCS objects: {e}")

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new GCS object(s) in {bucket_name}/{prefix} "
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

            return SensorResult(skip_reason="No new GCS objects found")

        return Definitions(sensors=[gcs_sensor])
