"""S3 Monitor Sensor Component.

Monitors an Amazon S3 bucket prefix for new objects and triggers jobs when objects are detected.
Passes object information via run_config to downstream assets.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from dagster import (
    AssetKey,
    AssetMaterialization,
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


# BEGIN dynamic-partition pruning (inlined per component; keep copies identical — see tests/test_dynamic_partition_pruning.py)
def _prune_stale_dynamic_partitions(state_path, keep_days, now, newly_added_keys):
    """Tracks {partition_key: first_seen_iso} in a local JSON file (independent of the
    sensor's own cursor) and returns (stale_keys, updated_state) -- the caller issues a
    DeleteDynamicPartitionsRequest for stale_keys and persists updated_state. Dynamic
    partitions otherwise accumulate forever. Returns ([], {}) immediately (no file I/O) when
    keep_days is falsy -- pruning is opt-in."""
    if not keep_days:
        return [], {}
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path) as f:
                state = json.load(f)
        except Exception:  # noqa: BLE001
            state = {}
    for k in newly_added_keys:
        state.setdefault(k, now.isoformat())
    cutoff = now - timedelta(days=keep_days)
    stale = [k for k, seen in state.items() if datetime.fromisoformat(seen) < cutoff]
    for k in stale:
        state.pop(k, None)
    return stale, state


def _write_prune_state(state_path, state):
    try:
        with open(state_path, "w") as f:
            json.dump(state, f)
    except Exception:  # noqa: BLE001
        pass
# END dynamic-partition pruning


class S3MonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring an S3 bucket prefix for new objects.

    This sensor monitors an S3 bucket prefix for new objects and triggers
    jobs when objects are detected. Object information is passed to downstream
    assets via run_config.

    Example:
        ```yaml
        type: dagster_community_components.S3MonitorSensorComponent
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
    dynamic_partition_keep_days: Optional[int] = Field(
        default=None,
        description=(
            "When set (partition_mode='dynamic_partition' or 'both'), partitions this sensor "
            "registered are deleted once older than this many days -- dynamic partitions "
            "otherwise accumulate forever. Tracked via a small local JSON state file "
            "(dynamic_partition_state_path), independent of the sensor's own cursor. Unset "
            "(default): no pruning."
        ),
    )
    dynamic_partition_state_path: Optional[str] = Field(
        default=None,
        description=(
            "Where dynamic_partition_keep_days persists its {partition_key: first_seen} "
            "state. Defaults to './{sensor_name}_dynamic_partition_state.json'. Ignored "
            "when dynamic_partition_keep_days is unset."
        ),
    )
    partition_key_template: str = Field(
        default="{key}",
        description=(
            "Template for the partition key registered per detected S3 object. "
            "Available fields: `{bucket}`, `{key}` (full S3 key), `{prefix}`, "
            "`{file_stem}` (filename without extension, e.g. `event-abc123`), "
            "`{file_name}` (full filename with extension, e.g. `event-abc123.json`), "
            "`{key_stem}` (key relative to prefix, with extension stripped — "
            "e.g. `2026/08/event-abc123` when prefix is `inbox/`). Default "
            "`{key}` makes the partition key equal to the full S3 object key "
            "(may contain slashes — valid but awkward for asset selectors). "
            "For a slash-free key parity with FilesystemMonitorSensorComponent, "
            "use `{file_stem}`."
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
            "Optional asset key (e.g. 's3_landing_zone') to receive an "
            "AssetObservation on every tick that yields new objects. Metadata "
            "includes bucket, prefix, new_objects, last_key, last_modified — "
            "lets an observable-source asset in the graph show a live timeline "
            "of object arrivals in the Dagster UI."
        ),
    )

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. External assets show healthy/green in the Dagster UI "
            "and downstream AutomationCondition.eager() fires naturally on "
            "parent updates. When False, emit AssetObservation — free of "
            "Dagster+ credit charges, but the target asset renders as "
            "observed-external (dashed border, gray) and downstream "
            "conditions that gate on ~any_deps_missing() (including "
            "eager()) will not fire. Both event types carry the same "
            "dagster/data_version tag."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='S3 Monitor Sensor', editable=True)

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
        dynamic_partition_keep_days = self.dynamic_partition_keep_days
        dynamic_partition_state_path = self.dynamic_partition_state_path or f"./{sensor_name}_dynamic_partition_state.json"
        partition_key_template = self.partition_key_template or "{key}"
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key

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
        def s3_sensor(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if self.emit_materialization else AssetObservation
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
            last_key: Optional[str] = None
            last_modified_iso: Optional[str] = None
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
                            from pathlib import PurePosixPath as _PP
                            _p = _PP(key)
                            _rel = key[len(prefix):] if prefix and key.startswith(prefix) else key
                            partition_key = partition_key_template.format(
                                bucket=bucket_name, key=key, prefix=prefix,
                                file_stem=_p.stem,
                                file_name=_p.name,
                                key_stem=str(_PP(_rel).with_suffix("")),
                            )
                            new_partition_keys.append(partition_key)
                            run_requests.append(
                                RunRequest(
                                    run_key=f"{bucket_name}/{key}-{etag}",
                                    partition_key=partition_key,
                                    run_config=(
                                        {"ops": {op_name: {"config": config_block}}}
                                        if partition_mode == "both" else None
                                    ),
                                )
                            )
                        else:  # run_config (default — existing behavior)
                            run_requests.append(
                                RunRequest(
                                    run_key=f"{bucket_name}/{key}-{etag}",
                                    run_config={"ops": {op_name: {"config": config_block}}},
                                )
                            )

                        latest_time = max(latest_time, last_modified)
                        last_key = key
                        last_modified_iso = last_modified.isoformat()

            except Exception as e:
                context.log.error(f"Error listing S3 objects in {bucket_name}/{prefix}: {e}")
                return SensorResult(skip_reason=f"Error listing S3 objects: {e}")

            dynamic_partitions_requests = (
                [AddDynamicPartitionsRequest(
                    partitions_def_name=dynamic_partitions_name or "",
                    partition_keys=new_partition_keys,
                )]
                if new_partition_keys and dynamic_partitions_name else []
            )

            # Pruning runs every tick (not just when new objects are found) -- stale
            # partitions must age out even on a quiet tick.
            if dynamic_partition_keep_days and dynamic_partitions_name:
                stale_keys, pruned_state = _prune_stale_dynamic_partitions(
                    dynamic_partition_state_path, dynamic_partition_keep_days,
                    datetime.now(timezone.utc), new_partition_keys,
                )
                if stale_keys:
                    from dagster import DeleteDynamicPartitionsRequest
                    dynamic_partitions_requests.append(
                        DeleteDynamicPartitionsRequest(
                            partitions_def_name=dynamic_partitions_name, partition_keys=stale_keys,
                        )
                    )
                    context.log.info(f"Pruned {len(stale_keys)} dynamic partition(s) older than {dynamic_partition_keep_days}d")
                _write_prune_state(dynamic_partition_state_path, pruned_state)

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new S3 object(s) in {bucket_name}/{prefix} "
                    f"(partition_mode={partition_mode})"
                )
                asset_events = (
                    [_event_cls(
                        asset_key=AssetKey.from_user_string(source_asset_key),
                        metadata={
                            "bucket": bucket_name,
                            "prefix": prefix,
                            "new_objects": len(run_requests),
                            "last_key": last_key or "",
                            "last_modified": last_modified_iso or "",
                        },
                    )]
                    if source_asset_key else []
                )
                return SensorResult(
                    run_requests=run_requests,
                    dynamic_partitions_requests=dynamic_partitions_requests,
                    cursor=latest_time.isoformat(),
                    asset_events=asset_events,
                )

            if dynamic_partitions_requests:
                return SensorResult(dynamic_partitions_requests=dynamic_partitions_requests)

            return SensorResult(skip_reason="No new S3 objects found")

        return Definitions(sensors=[s3_sensor])
