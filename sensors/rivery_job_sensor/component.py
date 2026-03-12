"""Rivery Job Sensor Component.

Polls the Rivery API and triggers a Dagster job when a specified river run
completes successfully. Rivery owns the schedule; Dagster reacts to completion.

Rivery API: https://{region}.rivery.io/api/v1
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class RiveryJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Rivery river run completes.

    Example:
        ```yaml
        type: dagster_component_templates.RiveryJobSensorComponent
        attributes:
          sensor_name: rivery_salesforce_done
          river_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          api_token_env_var: RIVERY_API_TOKEN
          job_name: downstream_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    river_id: str = Field(description="Rivery river (pipeline) ID to monitor")
    job_name: str = Field(description="Dagster job to trigger when river run completes")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Rivery API token")
    region: str = Field(default="us2", description="Rivery region (e.g. us2, eu1, ap1)")
    resource_key: Optional[str] = Field(default=None, description="Key of a RiveryResource")
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
        def rivery_job_sensor(context: SensorEvaluationContext):
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
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = f"https://{_self.region}.rivery.io/api/v1"
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

            try:
                resp = requests.get(f"{base}/rivers/{_self.river_id}/runs", headers=headers, params={"limit": 1}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                runs = data.get("runs", data if isinstance(data, list) else [])
                latest = runs[0] if runs else None
            except Exception as e:
                return SensorResult(skip_reason=f"Rivery API error: {e}")

            if not latest:
                return SensorResult(skip_reason="No runs found for this river")

            run_id = str(latest.get("runId", latest.get("id", "")))
            status = (latest.get("status") or latest.get("state") or "").lower()

            cursor = context.cursor or ""
            if status in ("success", "succeeded", "completed") and run_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "river_id": _self.river_id,
                            "run_id": run_id,
                        }}},
                    )],
                    cursor=run_id,
                )

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[rivery_job_sensor])
