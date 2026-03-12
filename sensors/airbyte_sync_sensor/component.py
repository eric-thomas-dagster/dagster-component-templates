"""Airbyte Sync Sensor Component.

Polls the Airbyte API and triggers a Dagster job when a connection
sync job completes successfully.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class AirbyteSyncSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a job when an Airbyte connection sync completes.

    Example:
        ```yaml
        type: dagster_component_templates.AirbyteSyncSensorComponent
        attributes:
          sensor_name: airbyte_orders_sync_done
          connection_id: aaaabbbb-cccc-dddd-eeee-ffffffffffff
          airbyte_url: http://localhost:8000
          job_name: process_synced_orders_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    connection_id: str = Field(description="Airbyte connection UUID")
    airbyte_url: str = Field(default="http://localhost:8000", description="Airbyte server URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Airbyte username (Cloud)")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Airbyte password (Cloud)")
    job_name: str = Field(description="Job to trigger when sync completes")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def airbyte_sync_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            auth = None
            if _self.username_env_var and _self.password_env_var:
                user = os.environ.get(_self.username_env_var, "")
                pwd = os.environ.get(_self.password_env_var, "")
                auth = (user, pwd)

            try:
                resp = requests.get(
                    f"{_self.airbyte_url}/api/v1/jobs/list",
                    json={"configTypes": ["sync"], "configId": _self.connection_id,
                          "includingJobId": 0, "pagination": {"pageSize": 1}},
                    auth=auth,
                    timeout=30,
                )
                resp.raise_for_status()
                jobs = resp.json().get("jobs", [])
            except Exception as e:
                return SensorResult(skip_reason=f"Airbyte API error: {e}")

            if not jobs:
                return SensorResult(skip_reason="No jobs found for this connection")

            job = jobs[0]
            job_info = job.get("job", {})
            job_id = str(job_info.get("id", ""))
            status = job_info.get("status", "")
            created_at = str(job_info.get("createdAt", ""))

            cursor = context.cursor or ""
            if status == "succeeded" and job_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=job_id,
                        run_config={"ops": {"config": {
                            "connection_id": _self.connection_id,
                            "airbyte_job_id": job_id,
                            "created_at": created_at,
                        }}},
                    )],
                    cursor=job_id,
                )

            return SensorResult(skip_reason=f"Job {job_id} status: {status}")

        return dg.Definitions(sensors=[airbyte_sync_sensor])
