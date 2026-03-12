"""ADLS Monitor Sensor Component.

Monitors an Azure Data Lake Storage Gen2 container for new files and triggers
jobs when files are detected. Passes file information via run_config to downstream assets.
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


class ADLSMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring Azure Data Lake Storage Gen2 for new files.

    This sensor monitors an ADLS Gen2 container directory for new files and triggers
    jobs when files are detected. File information is passed to downstream assets
    via run_config.

    Authentication uses DefaultAzureCredential by default, which supports managed
    identity, environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID,
    AZURE_CLIENT_SECRET), and az CLI login. Alternatively, provide a connection
    string via the AZURE_STORAGE_CONNECTION_STRING environment variable.

    Example:
        ```yaml
        type: dagster_component_templates.ADLSMonitorSensorComponent
        attributes:
          sensor_name: adls_data_sensor
          storage_account_name: mydatalake
          container_name: raw
          directory_path: incoming/
          file_pattern: ".*\\.parquet$"
          job_name: process_adls_files_job
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor"
    )

    storage_account_name: str = Field(
        description="Azure Storage account name (e.g., 'mydatalake')"
    )

    container_name: str = Field(
        description="Name of the ADLS Gen2 container (filesystem) to monitor"
    )

    directory_path: str = Field(
        default="",
        description="Directory path within the container to monitor (e.g., 'incoming/' or 'data/2024/')"
    )

    file_pattern: str = Field(
        default=".*",
        description="Regex pattern to match file names (e.g., '.*\\.parquet$' for Parquet files)"
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
        description="Whether to recursively list files in subdirectories"
    )

    connection_string_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the storage connection string. "
                    "If not provided, DefaultAzureCredential is used."
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
        storage_account_name = self.storage_account_name
        container_name = self.container_name
        directory_path = self.directory_path
        file_pattern = self.file_pattern
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        recursive = self.recursive
        connection_string_env_var = self.connection_string_env_var
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
        def adls_sensor(context: SensorEvaluationContext):
            """Sensor that monitors an ADLS Gen2 container for new files."""
            import os

            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError:
                return SensorResult(
                    skip_reason="azure-storage-file-datalake is not installed. "
                                "Run: pip install azure-storage-file-datalake azure-identity"
                )

            # Get cursor (last processed file timestamp as ISO string)
            cursor = context.cursor or "1970-01-01T00:00:00+00:00"
            last_processed_time = datetime.fromisoformat(cursor)
            if last_processed_time.tzinfo is None:
                last_processed_time = last_processed_time.replace(tzinfo=timezone.utc)

            # Compile file pattern
            try:
                pattern = re.compile(file_pattern)
            except re.error as e:
                context.log.error(f"Invalid regex pattern: {file_pattern}. Error: {e}")
                return SensorResult(skip_reason=f"Invalid regex pattern: {file_pattern}")

            # Build ADLS client
            try:
                if connection_string_env_var:
                    conn_str = os.environ.get(connection_string_env_var)
                    if not conn_str:
                        return SensorResult(
                            skip_reason=f"Environment variable '{connection_string_env_var}' is not set"
                        )
                    service_client = DataLakeServiceClient.from_connection_string(conn_str)
                else:
                    from azure.identity import DefaultAzureCredential
                    account_url = f"https://{storage_account_name}.dfs.core.windows.net"
                    service_client = DataLakeServiceClient(
                        account_url=account_url,
                        credential=DefaultAzureCredential(),
                    )
            except Exception as e:
                context.log.error(f"Failed to create ADLS client: {e}")
                return SensorResult(skip_reason=f"Failed to create ADLS client: {e}")

            # List files
            run_requests = []
            latest_time = last_processed_time

            try:
                fs_client = service_client.get_file_system_client(container_name)
                paths = fs_client.get_paths(
                    path=directory_path or "/",
                    recursive=recursive,
                )

                for path_item in paths:
                    # Skip directories
                    if path_item.is_directory:
                        continue

                    file_path = path_item.name
                    file_name = file_path.split("/")[-1]
                    last_modified = path_item.last_modified
                    size = path_item.content_length or 0

                    if last_modified is None:
                        continue

                    # Normalize timezone
                    if last_modified.tzinfo is None:
                        last_modified = last_modified.replace(tzinfo=timezone.utc)

                    # Skip if already processed
                    if last_modified <= last_processed_time:
                        continue

                    # Check pattern against file name
                    if not pattern.search(file_name):
                        continue

                    run_requests.append(
                        RunRequest(
                            run_key=f"{storage_account_name}/{container_name}/{file_path}-{last_modified.isoformat()}",
                            run_config={
                                "ops": {
                                    "config": {
                                        "storage_account": storage_account_name,
                                        "container": container_name,
                                        "file_path": file_path,
                                        "file_name": file_name,
                                        "size": size,
                                        "last_modified": last_modified.isoformat(),
                                        "directory_path": directory_path,
                                    }
                                }
                            },
                        )
                    )

                    latest_time = max(latest_time, last_modified)

            except Exception as e:
                context.log.error(
                    f"Error listing files in {storage_account_name}/{container_name}/{directory_path}: {e}"
                )
                return SensorResult(skip_reason=f"Error listing ADLS files: {e}")

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new file(s) in "
                    f"{storage_account_name}/{container_name}/{directory_path}"
                )
                return SensorResult(
                    run_requests=run_requests,
                    cursor=latest_time.isoformat(),
                )

            return SensorResult(skip_reason="No new ADLS files found")

        return Definitions(sensors=[adls_sensor])
