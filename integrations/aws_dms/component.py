"""AWS Database Migration Service (DMS) Component.

Import AWS DMS replication tasks as Dagster assets for database migrations and CDC.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class AWSDMSComponent(Component, Model, Resolvable):
    """Component for importing AWS DMS replication tasks as Dagster assets.

    Supports importing:
    - Replication Tasks (CDC and migration tasks)

    Example:
        ```yaml
        type: dagster_component_templates.AWSDMSComponent
        attributes:
          aws_region: us-east-1
          aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
          aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
          import_replication_tasks: true
        ```
    """

    aws_region: str = Field(
        description="AWS region (e.g., us-east-1)"
    )

    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID (optional if using IAM role)"
    )

    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key (optional if using IAM role)"
    )

    aws_session_token: Optional[str] = Field(
        default=None,
        description="AWS session token for temporary credentials (optional)"
    )

    import_replication_tasks: bool = Field(
        default=True,
        description="Import replication tasks as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    filter_by_tags: Optional[str] = Field(
        default=None,
        description="Comma-separated tag keys to filter entities (e.g., 'env,team')"
    )

    replication_task_type: str = Field(
        default="start-replication",
        description="Task start type: start-replication, resume-processing, or reload-target"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for replication task runs"
    )

    group_name: str = Field(
        default="aws_dms",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the AWS DMS component"
    )

    def _get_client(self):
        """Create AWS DMS client."""
        session_config = {"region_name": self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            session_config["aws_access_key_id"] = self.aws_access_key_id
            session_config["aws_secret_access_key"] = self.aws_secret_access_key

        if self.aws_session_token:
            session_config["aws_session_token"] = self.aws_session_token

        session = boto3.Session(**session_config)
        return session.client("dms")

    def _matches_filters(self, name: str, tags: Optional[List[Dict]] = None) -> bool:
        """Check if entity matches name and tag filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Tag filter
        if self.filter_by_tags and tags:
            required_keys = [k.strip() for k in self.filter_by_tags.split(",")]
            tag_keys = [tag["Key"] for tag in tags]
            if not all(key in tag_keys for key in required_keys):
                return False

        return True

    def _list_replication_tasks(self, client) -> List[Dict]:
        """List all replication tasks."""
        tasks = []
        paginator = client.get_paginator("describe_replication_tasks")

        for page in paginator.paginate():
            for task in page.get("ReplicationTasks", []):
                task_name = task.get("ReplicationTaskIdentifier", "")
                tags = task.get("Tags", [])

                if self._matches_filters(task_name, tags):
                    tasks.append({
                        "arn": task["ReplicationTaskArn"],
                        "name": task_name,
                        "status": task.get("Status", "unknown"),
                        "migration_type": task.get("MigrationType", "unknown"),
                    })

        return tasks

    def _get_replication_task_assets(self, client) -> List:
        """Generate replication task assets."""
        assets = []
        tasks = self._list_replication_tasks(client)

        for task_info in tasks:
            task_name = task_info["name"]
            task_arn = task_info["arn"]
            asset_key = f"dms_task_{task_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "task_name": task_name,
                    "task_arn": task_arn,
                    "migration_type": task_info["migration_type"],
                },
            )
            def replication_task_asset(
                context: AssetExecutionContext,
                task_name=task_name,
                task_arn=task_arn,
                migration_type=task_info["migration_type"],
            ):
                """Start AWS DMS replication task."""
                dms_client = self._get_client()

                # Get current task status
                response = dms_client.describe_replication_tasks(
                    Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}]
                )

                if not response["ReplicationTasks"]:
                    raise Exception(f"Replication task {task_name} not found")

                task = response["ReplicationTasks"][0]
                current_status = task["Status"]

                context.log.info(f"Current task status: {current_status}")

                # Start task if not running
                if current_status in ["ready", "stopped", "failed"]:
                    context.log.info(f"Starting replication task: {task_name}")

                    try:
                        start_response = dms_client.start_replication_task(
                            ReplicationTaskArn=task_arn,
                            StartReplicationTaskType=self.replication_task_type,
                        )

                        context.log.info(f"Task started: {start_response['ReplicationTask']['Status']}")

                        # Wait for task to reach running state
                        import time
                        max_wait = 600  # 10 minutes
                        elapsed = 0
                        poll_interval = 30

                        while elapsed < max_wait:
                            time.sleep(poll_interval)
                            elapsed += poll_interval

                            response = dms_client.describe_replication_tasks(
                                Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}]
                            )

                            if response["ReplicationTasks"]:
                                task = response["ReplicationTasks"][0]
                                status = task["Status"]
                                context.log.info(f"Task status: {status}")

                                if status == "running":
                                    break
                                elif status in ["failed", "stopped"]:
                                    raise Exception(f"Task failed to start: {status}")

                    except ClientError as e:
                        context.log.error(f"Error starting task: {e}")
                        raise

                # Get task statistics
                stats = task.get("ReplicationTaskStats", {})

                metadata = {
                    "task_name": task_name,
                    "status": task["Status"],
                    "migration_type": migration_type,
                    "start_time": str(task.get("ReplicationTaskStartDate", "")),
                    "full_load_progress": stats.get("FullLoadProgressPercent", 0),
                    "tables_loaded": stats.get("TablesLoaded", 0),
                    "tables_loading": stats.get("TablesLoading", 0),
                    "tables_errored": stats.get("TablesErrored", 0),
                }

                # CDC-specific metrics
                if migration_type in ["full-load-and-cdc", "cdc"]:
                    metadata.update({
                        "cdc_latency_source": stats.get("CDCLatencySource", 0),
                        "cdc_latency_target": stats.get("CDCLatencyTarget", 0),
                    })

                return metadata

            assets.append(replication_task_asset)

        return assets

    def _get_observation_sensor(self, client):
        """Generate sensor to observe replication task runs."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def dms_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe AWS DMS replication task status."""
            dms_client = self._get_client()

            # Get all replication tasks
            tasks = self._list_replication_tasks(dms_client)

            for task_info in tasks:
                task_name = task_info["name"]
                task_arn = task_info["arn"]

                # Get task details
                response = dms_client.describe_replication_tasks(
                    Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}]
                )

                if not response["ReplicationTasks"]:
                    continue

                task = response["ReplicationTasks"][0]
                status = task["Status"]

                # Emit materialization for running tasks with progress
                if status in ["running", "stopped", "failed"]:
                    stats = task.get("ReplicationTaskStats", {})
                    asset_key = f"dms_task_{task_name}"

                    metadata = {
                        "task_name": MetadataValue.text(task_name),
                        "status": MetadataValue.text(status),
                        "migration_type": MetadataValue.text(task_info["migration_type"]),
                        "full_load_progress": MetadataValue.float(
                            stats.get("FullLoadProgressPercent", 0)
                        ),
                        "tables_loaded": MetadataValue.int(stats.get("TablesLoaded", 0)),
                        "tables_loading": MetadataValue.int(stats.get("TablesLoading", 0)),
                        "tables_errored": MetadataValue.int(stats.get("TablesErrored", 0)),
                    }

                    # CDC-specific metrics
                    if task_info["migration_type"] in ["full-load-and-cdc", "cdc"]:
                        metadata.update({
                            "cdc_latency_source": MetadataValue.float(
                                stats.get("CDCLatencySource", 0)
                            ),
                            "cdc_latency_target": MetadataValue.float(
                                stats.get("CDCLatencyTarget", 0)
                            ),
                        })

                    yield AssetMaterialization(
                        asset_key=asset_key,
                        metadata=metadata,
                    )

        return dms_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        client = self._get_client()

        assets = []
        sensors = []

        # Import replication tasks
        if self.import_replication_tasks:
            assets.extend(self._get_replication_task_assets(client))

        # Generate observation sensor
        if self.generate_sensor and self.import_replication_tasks:
            sensors.append(self._get_observation_sensor(client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
