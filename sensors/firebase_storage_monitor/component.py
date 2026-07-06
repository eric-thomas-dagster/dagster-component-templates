"""Firebase Storage Monitor sensor component.

Watches a Firebase Storage bucket (which is really Google Cloud Storage under
the hood) for new files and emits one RunRequest per new object. Cursor-based
deduplication uses the object's `updated` timestamp.
"""

import re
from datetime import datetime, timezone
from typing import Optional

import dagster as dg
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class FirebaseStorageMonitorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watch a Firebase Storage bucket for new files.

    Uses the paired `firebase_resource` for authenticated bucket access.
    Firebase Storage is Cloud Storage under the hood, so the underlying
    `bucket.list_blobs()` semantics are identical to GCS.

    Emits one RunRequest per new blob detected since the last cursor.
    Optionally registers detected blob names as a dynamic partition set.
    """

    sensor_name: str = Field(description="Unique name for this sensor.")
    bucket_name: str = Field(
        description="Firebase Storage bucket name (e.g. '<project>.appspot.com'). "
                    "Falls back to the FirebaseResource's default bucket when empty.",
    )
    resource_name: str = Field(
        default="firebase_resource",
        description="Name of the FirebaseResource this sensor uses.",
    )
    prefix: str = Field(
        default="",
        description="Object prefix to scope monitoring (e.g. 'uploads/').",
    )
    blob_pattern: str = Field(
        default=".*",
        description="Regex matched against the blob name relative to the prefix.",
    )
    job_name: Optional[str] = Field(
        default=None,
        description="Optional name of the job to trigger on detection.",
    )
    minimum_interval_seconds: int = Field(
        default=60,
        description="Minimum interval between sensor evaluations.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="If set, register detected blob names as dynamic partitions "
                    "under this name and yield RunRequest(partition_key=<name>).",
    )
    default_status: str = Field(
        default="stopped",
        description="Initial sensor status: 'running' or 'stopped'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sensor_name = self.sensor_name
        bucket_name = self.bucket_name
        resource_name = self.resource_name
        prefix = self.prefix
        blob_pattern = self.blob_pattern
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        dynamic_partition_name = self.dynamic_partition_name

        if self.default_status not in ("running", "stopped"):
            raise ValueError(
                f"default_status must be 'running' or 'stopped', got {self.default_status!r}"
            )
        default_status = (
            DefaultSensorStatus.RUNNING
            if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        # Validate the regex at load time — fails fast in `dg check`.
        try:
            re.compile(blob_pattern)
        except re.error as e:
            raise ValueError(f"blob_pattern is not a valid regex: {e}")

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys={resource_name},
        )
        def firebase_storage_sensor(context: dg.SensorEvaluationContext):
            fb = getattr(context.resources, resource_name)
            try:
                bucket = fb.get_storage(bucket_name or None)
            except Exception as e:
                context.log.error(f"Failed to obtain Firebase Storage bucket: {e}")
                return dg.SensorResult(skip_reason=f"Failed to obtain bucket: {e}")

            cursor_str = context.cursor or "1970-01-01T00:00:00+00:00"
            last_processed = datetime.fromisoformat(cursor_str)
            if last_processed.tzinfo is None:
                last_processed = last_processed.replace(tzinfo=timezone.utc)

            pattern = re.compile(blob_pattern)

            run_requests: list = []
            new_partition_keys: list = []
            latest = last_processed

            try:
                blobs = bucket.list_blobs(prefix=prefix)
                for blob in blobs:
                    updated = blob.updated
                    if updated is None:
                        continue
                    if updated.tzinfo is None:
                        updated = updated.replace(tzinfo=timezone.utc)
                    if updated <= last_processed:
                        continue
                    if blob.name.endswith("/"):
                        continue

                    relative_name = (
                        blob.name[len(prefix):] if blob.name.startswith(prefix) else blob.name
                    )
                    if not pattern.search(relative_name):
                        continue

                    config_block = {
                        "bucket": bucket.name,
                        "name": blob.name,
                        "size": blob.size or 0,
                        "etag": blob.etag or "",
                        "updated": updated.isoformat(),
                        "content_type": blob.content_type or "",
                        "prefix": prefix,
                    }

                    if dynamic_partition_name:
                        new_partition_keys.append(blob.name)
                        run_requests.append(
                            dg.RunRequest(
                                run_key=f"{bucket.name}/{blob.name}-{blob.etag}",
                                partition_key=blob.name,
                            )
                        )
                    else:
                        run_requests.append(
                            dg.RunRequest(
                                run_key=f"{bucket.name}/{blob.name}-{blob.etag}",
                                run_config={"ops": {"config": config_block}},
                            )
                        )
                    if updated > latest:
                        latest = updated
            except Exception as e:
                context.log.error(f"Error listing Firebase Storage bucket: {e}")
                return dg.SensorResult(skip_reason=f"Error listing bucket: {e}")

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new object(s) in "
                    f"firebase-storage://{bucket.name}/{prefix}"
                )
                partition_requests = (
                    [dg.AddDynamicPartitionsRequest(
                        partitions_def_name=dynamic_partition_name,
                        partition_keys=new_partition_keys,
                    )]
                    if dynamic_partition_name and new_partition_keys else []
                )
                return dg.SensorResult(
                    run_requests=run_requests,
                    dynamic_partitions_requests=partition_requests,
                    cursor=latest.isoformat(),
                )
            return dg.SensorResult(skip_reason="No new files found")

        return dg.Definitions(sensors=[firebase_storage_sensor])
