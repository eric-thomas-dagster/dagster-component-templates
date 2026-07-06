"""Supabase Storage Monitor Sensor Component.

Watches a Supabase Storage bucket (optionally scoped by prefix) for new
objects and emits a RunRequest per new file, using the object path as the
run key. Cursor-tracked by the newest observed ``updated_at`` timestamp.

Pairs with the Supabase Storage REST/JS-parity API exposed via
``supabase.Client.storage.from_(bucket).list(path, options)``.
"""

from datetime import datetime, timezone
from typing import Optional

from dagster import (
    AddDynamicPartitionsRequest,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SupabaseStorageMonitorComponent(Component, Model, Resolvable):
    """Monitor a Supabase Storage bucket for new files.

    Emits one ``RunRequest`` per newly observed object. When
    ``dynamic_partition_name`` is set, each new object path is also
    registered as a dynamic partition and the RunRequest is keyed on the
    object path.
    """

    sensor_name: str = Field(description="Unique name for this sensor")
    bucket_name: str = Field(description="Supabase Storage bucket to monitor")
    prefix: str = Field(
        default="",
        description="Object prefix (folder) to scope monitoring, e.g. 'incoming/' or 'raw/2026/'.",
    )
    resource_name: str = Field(
        default="supabase_resource",
        description="Resource key of the SupabaseResource providing the client.",
    )
    minimum_interval_seconds: int = Field(
        default=60,
        description="Minimum time (in seconds) between sensor evaluations.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description=(
            "Optional name of a DynamicPartitionsDefinition. When set, each new "
            "object path is registered as a dynamic partition and RunRequests are "
            "keyed on the object path."
        ),
    )
    job_name: Optional[str] = Field(
        default=None,
        description="Job to trigger. Leave unset when downstream assets are activated via auto-materialize.",
    )
    default_status: str = Field(
        default="stopped",
        description="Default sensor status: 'running' or 'stopped'.",
    )
    page_size: int = Field(
        default=1000,
        description="Page size for storage.list() calls. Supabase caps at 1000.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        bucket_name = self.bucket_name
        prefix = (self.prefix or "").rstrip("/")
        resource_name = self.resource_name
        minimum_interval_seconds = self.minimum_interval_seconds
        dyn_part_name = self.dynamic_partition_name
        job_name = self.job_name
        page_size = max(1, min(self.page_size, 1000))

        default_status = (
            DefaultSensorStatus.RUNNING
            if self.default_status.lower() == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys={resource_name},
        )
        def _supabase_storage_sensor(context: SensorEvaluationContext):
            supabase_resource = getattr(context.resources, resource_name)
            client = supabase_resource.get_client()

            cursor = context.cursor or "1970-01-01T00:00:00+00:00"
            try:
                last_seen = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
            except ValueError:
                last_seen = datetime(1970, 1, 1, tzinfo=timezone.utc)
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)

            # Walk the bucket recursively so nested folders are seen.
            new_files: list[dict] = []
            latest = last_seen
            stack: list[str] = [prefix]
            try:
                while stack:
                    folder = stack.pop()
                    offset = 0
                    while True:
                        items = client.storage.from_(bucket_name).list(
                            folder,
                            {
                                "limit": page_size,
                                "offset": offset,
                                "sortBy": {"column": "updated_at", "order": "asc"},
                            },
                        )
                        if not items:
                            break
                        for item in items:
                            name = item.get("name") or ""
                            # A "folder" placeholder in Supabase Storage has id=None
                            # and metadata=None. Recurse into them.
                            item_id = item.get("id")
                            metadata = item.get("metadata")
                            full_path = f"{folder}/{name}" if folder else name
                            if item_id is None and metadata is None:
                                stack.append(full_path)
                                continue
                            updated_str = (
                                item.get("updated_at")
                                or item.get("created_at")
                                or ""
                            )
                            try:
                                updated_at = datetime.fromisoformat(
                                    updated_str.replace("Z", "+00:00")
                                )
                            except ValueError:
                                continue
                            if updated_at.tzinfo is None:
                                updated_at = updated_at.replace(tzinfo=timezone.utc)
                            if updated_at <= last_seen:
                                continue
                            size = (metadata or {}).get("size", 0)
                            mime = (metadata or {}).get("mimetype", "")
                            new_files.append(
                                {
                                    "path": full_path,
                                    "size": size,
                                    "mimetype": mime,
                                    "updated_at": updated_at.isoformat(),
                                    "etag": (metadata or {}).get("eTag", "") or item.get("id") or "",
                                }
                            )
                            if updated_at > latest:
                                latest = updated_at
                        if len(items) < page_size:
                            break
                        offset += page_size
            except Exception as e:
                context.log.error(f"Error listing Supabase Storage bucket {bucket_name!r}: {e}")
                return SensorResult(skip_reason=f"Error listing Supabase Storage bucket: {e}")

            if not new_files:
                return SensorResult(skip_reason="No new Supabase Storage objects found")

            run_requests: list[RunRequest] = []
            new_partition_keys: list[str] = []
            for f in new_files:
                run_key = f"{bucket_name}/{f['path']}-{f['etag']}"
                config_block = {
                    "bucket": bucket_name,
                    "path": f["path"],
                    "size": f["size"],
                    "mimetype": f["mimetype"],
                    "updated_at": f["updated_at"],
                    "prefix": prefix,
                }
                if dyn_part_name:
                    new_partition_keys.append(f["path"])
                    run_requests.append(
                        RunRequest(
                            run_key=run_key,
                            partition_key=f["path"],
                        )
                    )
                else:
                    run_requests.append(
                        RunRequest(
                            run_key=run_key,
                            run_config={"ops": {"config": config_block}},
                        )
                    )

            context.log.info(
                f"Found {len(new_files)} new Supabase Storage object(s) in "
                f"{bucket_name}/{prefix or '(root)'}"
            )

            dynamic_partitions_requests = (
                [
                    AddDynamicPartitionsRequest(
                        partitions_def_name=dyn_part_name,
                        partition_keys=new_partition_keys,
                    )
                ]
                if new_partition_keys and dyn_part_name
                else []
            )

            return SensorResult(
                run_requests=run_requests,
                dynamic_partitions_requests=dynamic_partitions_requests,
                cursor=latest.isoformat(),
            )

        return Definitions(sensors=[_supabase_storage_sensor])
