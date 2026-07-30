"""Filesystem Monitor Sensor Component.

Monitors a local filesystem directory for new files and triggers jobs when files are detected.
Passes file information via run_config to downstream assets.
"""

import os
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


class FilesystemMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring filesystem directories for new files.

    This sensor monitors a local directory for new files matching a pattern and triggers
    jobs when files are detected. File information is passed to downstream assets via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.FilesystemMonitorSensorComponent
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
        def filesystem_sensor(context: SensorEvaluationContext):
            """Sensor that monitors a directory for new files."""

            # Ensure directory exists
            if not os.path.exists(directory_path):
                context.log.warning(f"Directory does not exist: {directory_path}")
                return SensorResult(skip_reason=f"Directory does not exist: {directory_path}")

            if not os.path.isdir(directory_path):
                context.log.warning(f"Path is not a directory: {directory_path}")
                return SensorResult(skip_reason=f"Path is not a directory: {directory_path}")

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

                    file_name = os.path.basename(file_path)
                    file_stem = os.path.splitext(file_name)[0]

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
                        # Legacy run_config shape — for jobs whose op reads
                        # file_path / file_name / file_size from config.
                        run_requests.append(
                            RunRequest(
                                run_key=f"{file_path}-{mtime}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "file_path": file_path,
                                            "file_name": file_name,
                                            "file_size": stat.st_size,
                                            "file_modified_time": mtime,
                                            "directory_path": directory_path,
                                        }
                                    }
                                },
                            )
                        )

                    # Track latest modification time
                    latest_mtime = max(latest_mtime, mtime)

                except Exception as e:
                    context.log.error(f"Error processing file {file_path}: {e}")
                    continue

            if run_requests:
                context.log.info(f"Found {len(run_requests)} new file(s) matching pattern")
                dynamic_partitions_requests = (
                    [AddDynamicPartitionsRequest(
                        partitions_def_name=dynamic_partitions_name or "",
                        partition_keys=new_partition_keys,
                    )]
                    if new_partition_keys and dynamic_partitions_name else []
                )
                return SensorResult(
                    run_requests=run_requests,
                    cursor=str(latest_mtime),
                    dynamic_partitions_requests=dynamic_partitions_requests,
                )

            return SensorResult(skip_reason="No new files found")

        return Definitions(sensors=[filesystem_sensor])
