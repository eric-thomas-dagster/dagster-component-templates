"""Precisely Job Sensor Component.

Polls the Precisely Connect REST API and triggers a Dagster job when a specified
Precisely job run completes successfully. Precisely owns the schedule; Dagster reacts.

Precisely Connect REST API: https://{host}/api
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class PreciselyJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Precisely Connect job run completes.

    Example:
        ```yaml
        type: dagster_component_templates.PreciselyJobSensorComponent
        attributes:
          sensor_name: precisely_etl_done
          job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          job_name: downstream_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    job_id: str = Field(description="Precisely Connect job ID to monitor")
    job_name: str = Field(description="Dagster job to trigger when Precisely run completes")
    host_env_var: Optional[str] = Field(default=None, description="Env var with Precisely Connect host URL")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Precisely API token")
    resource_key: Optional[str] = Field(default=None, description="Key of a PreciselyResource")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key} if self.resource_key else set()
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
        def precisely_job_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key)
                base = resource._base()
                headers = resource._headers()
            else:
                host = os.environ.get(_self.host_env_var or "", "").rstrip("/")
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = host
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

            try:
                resp = requests.get(
                    f"{base}/api/v1/jobs/{_self.job_id}/runs",
                    headers=headers,
                    params={"limit": 1, "sort": "-startTime"},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                runs = data.get("runs", data if isinstance(data, list) else [])
                latest = runs[0] if runs else None
            except Exception as e:
                return SensorResult(skip_reason=f"Precisely API error: {e}")

            if not latest:
                return SensorResult(skip_reason="No runs found for this job")

            run_id = str(latest.get("runId", latest.get("id", "")))
            status = (latest.get("status") or latest.get("state") or "").upper()

            cursor = context.cursor or ""
            if status in ("SUCCESS", "COMPLETED", "SUCCEEDED") and run_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "job_id": _self.job_id,
                            "run_id": run_id,
                        }}},
                    )],
                    cursor=run_id,
                )

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[precisely_job_sensor])
