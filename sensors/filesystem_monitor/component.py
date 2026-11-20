"""Filesystem Monitor Sensor Component.

Monitors a local filesystem directory for new files and triggers jobs when files are detected.
Passes file information via run_config to downstream assets.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import re

from dagster import (
    AddDynamicPartitionsRequest,
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


# BEGIN file-size-stability check (local to this component -- see min_age_seconds/min_size_bytes/require_stable_size)
def _check_file_size_stability(state_path, current_sizes: dict) -> tuple:
    """Tracks {file_path: last_seen_size_bytes} in a local JSON state file (independent of the
    sensor's own mtime cursor) across ticks. Returns (stable_paths, updated_state): a path is
    "stable" when its size in `current_sizes` exactly matches what was recorded on a PRIOR tick --
    i.e. the file hasn't grown or shrunk since the last time this sensor looked, so it isn't still
    being written to or transferred in. A path seen for the first time is never stable (nothing to
    compare against yet), so every file gets at least one full tick of delay before it can fire.
    `updated_state` keeps only paths present in `current_sizes` this tick, so entries for files
    that were processed, deleted, or renamed don't accumulate forever."""
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path) as f:
                state = json.load(f)
        except Exception:  # noqa: BLE001
            state = {}
    stable = {path for path, size in current_sizes.items() if state.get(path) == size}
    return stable, dict(current_sizes)


def _write_json_state(state_path, state):
    try:
        with open(state_path, "w") as f:
            json.dump(state, f)
    except Exception:  # noqa: BLE001
        pass
# END file-size-stability check


class FilesystemMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring filesystem directories for new files.

    This sensor monitors a local directory for new files matching a pattern and triggers
    jobs when files are detected. File information is passed to downstream assets via run_config.

    Example:
        ```yaml
        type: dagster_community_components.FilesystemMonitorSensorComponent
        attributes:
          sensor_name: data_files_sensor
          directory_path: /data/incoming
          file_pattern: ".*\\.csv$"
          job_name: process_files_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor"
    )

    directory_path: str = Field(
        description="Path to the directory to monitor for new files"
    )

    file_pattern: str = Field(
        default=".*",
        description="Regex pattern to match files (e.g., '.*\\.csv$' for CSV files)"
    )

    job_name: str = Field(
        description="Name of the job to trigger when files are detected"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    recursive: bool = Field(
        default=False,
        description="Whether to recursively search subdirectories"
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    partition_mode: str = Field(
        default="run_config",
        description=(
            "How the sensor surfaces detected files: 'run_config' (default — "
            "embed file metadata in run_config), 'static_partition' (yield "
            "RunRequest(partition_key=<from partition_key_template>) — use when "
            "the target asset has a static/dynamic partitions_def), or "
            "'dynamic_partition' (also registers each key on the given "
            "DynamicPartitionsDefinition first)."
        ),
    )
    partition_key_template: str = Field(
        default="{file_stem}",
        description=(
            "Template for the partition key per detected file. Fields: "
            "{file_path}, {file_name}, {file_stem} (filename without extension). "
            "Default `{file_stem}` — e.g. `/tmp/approvals/t3.json` → `t3`."
        ),
    )
    dynamic_partitions_name: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_mode='dynamic_partition'. Must match the "
            "downstream asset's `dynamic_partition_name:` field."
        ),
    )
    dynamic_partition_keep_days: Optional[int] = Field(
        default=None,
        description=(
            "When set (partition_mode='dynamic_partition'), partitions this sensor "
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

    min_age_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Skip a file whose mtime is younger than this many seconds -- it may still be "
            "being written or transferred in. Checked against the file's mtime at the time of "
            "this tick, not when it first appeared. Unset (default): no age check."
        ),
    )
    min_size_bytes: Optional[int] = Field(
        default=None,
        description="Skip a file smaller than this many bytes -- catches a file that exists but hasn't finished being written/copied yet. Unset (default): no size check.",
    )
    require_stable_size: bool = Field(
        default=False,
        description=(
            "Skip a file whose size has changed since the last tick this sensor ran (tracked "
            "via file_stability_state_path, independent of the mtime cursor) -- catches an "
            "in-progress write/transfer even when mtime keeps advancing (e.g. a slow copy that "
            "touches the file continuously) and min_age_seconds alone wouldn't. A file seen for "
            "the first time is always skipped and re-checked next tick, so every file gets at "
            "least one full tick of delay before it can ever fire."
        ),
    )
    file_stability_state_path: Optional[str] = Field(
        default=None,
        description=(
            "Where require_stable_size persists its {file_path: last_seen_size} state. Defaults "
            "to './{sensor_name}_file_stability_state.json'. Ignored when require_stable_size is "
            "false."
        ),
    )

    batch_into_one_run: bool = Field(
        default=False,
        description=(
            "Launch ONE run per check that found new files, instead of one per file -- for a job that processes the whole "
            "folder each time (e.g. an extractor listing a directory), where a run per file would repeat the same work. "
            "While a run of the job is still going the sensor waits (without moving its cursor), so files that arrive "
            "meanwhile are picked up by the next run rather than processed twice. Only with partition_mode='run_config'."
        ),
    )

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab
        (dagster>=1.13.8 + `flagComponentInstanceUI`)."""
        from dagster.components.resolved.form_config import ComponentFormConfig

        return ComponentFormConfig(label="Filesystem Monitor Sensor", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        directory_path = self.directory_path
        file_pattern = self.file_pattern
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        recursive = self.recursive
        default_status_str = self.default_status
        partition_mode = self.partition_mode
        partition_key_template = self.partition_key_template
        dynamic_partitions_name = self.dynamic_partitions_name
        dynamic_partition_keep_days = self.dynamic_partition_keep_days
        dynamic_partition_state_path = self.dynamic_partition_state_path or f"./{sensor_name}_dynamic_partition_state.json"
        batch_into_one_run = self.batch_into_one_run
        min_age_seconds = self.min_age_seconds
        min_size_bytes = self.min_size_bytes
        require_stable_size = self.require_stable_size
        file_stability_state_path = self.file_stability_state_path or f"./{sensor_name}_file_stability_state.json"

        if batch_into_one_run and partition_mode != "run_config":
            raise ValueError(
                "filesystem_monitor: batch_into_one_run launches a single unpartitioned run per check, so it needs "
                f"partition_mode='run_config' (got {partition_mode!r})."
            )

        if partition_mode == "dynamic_partition" and not dynamic_partitions_name:
            raise ValueError(
                "filesystem_monitor: partition_mode='dynamic_partition' requires "
                "dynamic_partitions_name (must match downstream asset's "
                "dynamic_partition_name)."
            )

        # Convert default_status string to enum
        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
        )
        def filesystem_sensor(context: SensorEvaluationContext, **_resources):
            """Sensor that monitors a directory for new files."""

            # Ensure directory exists
            if not os.path.exists(directory_path):
                context.log.warning(f"Directory does not exist: {directory_path}")
                return SensorResult(skip_reason=f"Directory does not exist: {directory_path}")

            if not os.path.isdir(directory_path):
                context.log.warning(f"Path is not a directory: {directory_path}")
                return SensorResult(skip_reason=f"Path is not a directory: {directory_path}")

            if batch_into_one_run:
                from dagster import DagsterRunStatus, RunsFilter

                in_progress = [
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.MANAGED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                ]
                if context.instance.get_runs(filters=RunsFilter(job_name=job_name, statuses=in_progress), limit=1):
                    return SensorResult(
                        skip_reason=f"A run of {job_name} is still in progress; new files are picked up when it finishes"
                    )

            # Get cursor (last processed file timestamp)
            cursor = context.cursor or "0"
            last_processed_time = float(cursor)

            # Compile regex pattern
            try:
                pattern = re.compile(file_pattern)
            except re.error as e:
                context.log.error(f"Invalid regex pattern: {file_pattern}. Error: {e}")
                return SensorResult(skip_reason=f"Invalid regex pattern: {file_pattern}")

            # Find new files
            run_requests = []
            latest_mtime = last_processed_time

            # Get all files
            if recursive:
                files = []
                for root, _, filenames in os.walk(directory_path):
                    for filename in filenames:
                        files.append(os.path.join(root, filename))
            else:
                files = [
                    os.path.join(directory_path, f)
                    for f in os.listdir(directory_path)
                    if os.path.isfile(os.path.join(directory_path, f))
                ]

            # Process each file
            new_partition_keys: list = []
            # Pass 1: stat every candidate file (pattern match + mtime-cursor + age/size checks).
            # Arrival-stability (require_stable_size) needs every candidate's size collected
            # BEFORE deciding which are stable -- one state-file read/write per tick, not one
            # per file -- so it's applied as a second pass below, not inline here.
            now_ts = datetime.now(timezone.utc).timestamp()
            candidates: list = []  # (file_path, mtime, size, file_name, file_stem)
            for file_path in files:
                try:
                    # Check if file matches pattern
                    if not pattern.search(os.path.basename(file_path)):
                        continue

                    # Get file stats
                    stat = os.stat(file_path)
                    mtime = stat.st_mtime

                    # Skip if already processed
                    if mtime <= last_processed_time:
                        continue

                    # Skip a file that might still be being written/transferred in.
                    if min_age_seconds and (now_ts - mtime) < min_age_seconds:
                        continue
                    size = stat.st_size
                    if min_size_bytes and size < min_size_bytes:
                        continue

                    file_name = os.path.basename(file_path)
                    file_stem = os.path.splitext(file_name)[0]
                    candidates.append((file_path, mtime, size, file_name, file_stem))
                except Exception as e:
                    context.log.error(f"Error processing file {file_path}: {e}")
                    continue

            # Pass 2: arrival-stability -- only a file whose size matches what was recorded on a
            # PRIOR tick counts as done writing. A file seen for the first time is held back
            # (added to the state file, but filtered out of candidates) until at least one more
            # tick confirms its size stopped changing.
            if require_stable_size and candidates:
                stable_paths, updated_stability_state = _check_file_size_stability(
                    file_stability_state_path, {c[0]: c[2] for c in candidates}
                )
                _write_json_state(file_stability_state_path, updated_stability_state)
                candidates = [c for c in candidates if c[0] in stable_paths]

            for file_path, mtime, size, file_name, file_stem in candidates:
                try:
                    if partition_mode in ("static_partition", "dynamic_partition"):
                        # Materialize a specific partition of the downstream asset(s).
                        partition_key = partition_key_template.format(
                            file_path=file_path,
                            file_name=file_name,
                            file_stem=file_stem,
                        )
                        new_partition_keys.append(partition_key)
                        run_requests.append(
                            RunRequest(
                                run_key=f"{partition_key}-{mtime}",
                                partition_key=partition_key,
                            )
                        )
                    else:
                        # Default: launch the job with no run_config. Modern
                        # Dagster requires `ops` config to be keyed by op
                        # name (`{ops: {<op_name>: {config: ...}}}`), which
                        # we can't compute generically from a job_name alone.
                        # If your job needs file metadata as config, wire the
                        # sensor's target job to accept a `resources: {run_files:
                        # ...}` block instead of ops config, or use
                        # partition_mode='static_partition' + a partition key
                        # derived from the filename.
                        run_requests.append(
                            RunRequest(
                                run_key=f"{file_path}-{mtime}",
                            )
                        )

                    # Track latest modification time
                    latest_mtime = max(latest_mtime, mtime)

                except Exception as e:
                    context.log.error(f"Error processing file {file_path}: {e}")
                    continue

            dynamic_partitions_requests = (
                [AddDynamicPartitionsRequest(
                    partitions_def_name=dynamic_partitions_name or "",
                    partition_keys=new_partition_keys,
                )]
                if new_partition_keys and dynamic_partitions_name else []
            )

            # Pruning runs every tick (not just when new files are found) -- stale
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
                context.log.info(f"Found {len(run_requests)} new file(s) matching pattern")
                if batch_into_one_run:
                    run_requests = [RunRequest(run_key=f"batch-{latest_mtime}-{len(run_requests)}")]
                return SensorResult(
                    run_requests=run_requests,
                    cursor=str(latest_mtime),
                    dynamic_partitions_requests=dynamic_partitions_requests,
                )

            if dynamic_partitions_requests:
                return SensorResult(dynamic_partitions_requests=dynamic_partitions_requests)

            return SensorResult(skip_reason="No new files found")

        return Definitions(sensors=[filesystem_sensor])
