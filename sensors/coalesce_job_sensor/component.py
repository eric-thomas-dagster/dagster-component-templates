"""Coalesce Job Sensor Component.

Polls the Coalesce Scheduler API and triggers a Dagster job when a specified
Coalesce job run completes successfully.

Coalesce owns the schedule; Dagster reacts to completion —
the same pattern as dbt_cloud_job_sensor and fivetran_sync_sensor.

API: https://app.coalescesoftware.io/scheduler
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class CoalesceJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Coalesce job run completes.

    Example:
        ```yaml
        type: dagster_component_templates.CoalesceJobSensorComponent
        attributes:
          sensor_name: coalesce_transform_done
          environment_id: "my-env-id"
          job_id: "my-job-id"
          api_token_env_var: COALESCE_API_TOKEN
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    environment_id: str = Field(description="Coalesce environment ID")
    job_id: str = Field(description="Coalesce job ID to monitor")
    api_token_env_var: str = Field(description="Env var containing the Coalesce API token")
    job_name: str = Field(description="Dagster job to trigger when Coalesce run succeeds")
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
        def coalesce_job_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            token = os.environ.get(_self.api_token_env_var, "")
            headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
            base = "https://app.coalescesoftware.io"

            try:
                resp = requests.get(
                    f"{base}/scheduler/runs",
                    headers=headers,
                    params={
                        "environmentID": _self.environment_id,
                        "jobID": _self.job_id,
                        "limit": 1,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                runs = resp.json().get("data", resp.json() if isinstance(resp.json(), list) else [])
            except Exception as e:
                return SensorResult(skip_reason=f"Coalesce API error: {e}")

            if not runs:
                return SensorResult(skip_reason="No runs found for this job")

            latest = runs[0]
            run_counter = str(latest.get("runCounter", latest.get("id", "")))
            status = (latest.get("status") or "").lower()

            cursor = context.cursor or ""
            if status == "succeeded" and run_counter != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_counter,
                        run_config={"ops": {"config": {
                            "environment_id": _self.environment_id,
                            "job_id": _self.job_id,
                            "run_counter": int(run_counter),
                        }}},
                    )],
                    cursor=run_counter,
                )

            return SensorResult(skip_reason=f"Run {run_counter} status: {status}")

        return dg.Definitions(sensors=[coalesce_job_sensor])
