"""Matillion Job Sensor Component.

Polls the Matillion ETL REST API and triggers a Dagster job when a specified
Matillion job run completes successfully.

Matillion owns the schedule; Dagster reacts to completion.

Matillion ETL REST API: https://{instance}/rest/v1
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class MatillionJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Matillion ETL job run completes.

    Example:
        ```yaml
        type: dagster_component_templates.MatillionJobSensorComponent
        attributes:
          sensor_name: matillion_orders_done
          project: MyProject
          version: default
          job_name: Load Orders
          instance_url_env_var: MATILLION_INSTANCE_URL
          username_env_var: MATILLION_USERNAME
          password_env_var: MATILLION_PASSWORD
          job_name_dagster: downstream_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    project: str = Field(description="Matillion project name")
    version: str = Field(default="default", description="Matillion project version name")
    job_name: str = Field(description="Matillion job name to monitor")
    job_name_dagster: str = Field(description="Dagster job to trigger when Matillion run completes")
    instance_url_env_var: Optional[str] = Field(default=None, description="Env var with Matillion instance URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Matillion username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Matillion password")
    resource_key: Optional[str] = Field(default=None, description="Key of a MatillionResource")
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
            job_name=_self.job_name_dagster,
            required_resource_keys=required_resource_keys,
        )
        def matillion_job_sensor(context: SensorEvaluationContext):
            import os, base64
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key)
                try:
                    latest = resource.get_latest_run(_self.project, _self.version, _self.job_name)
                except Exception as e:
                    return SensorResult(skip_reason=f"Matillion API error: {e}")
            else:
                instance_url = os.environ.get(_self.instance_url_env_var or "", "").rstrip("/")
                username = os.environ.get(_self.username_env_var or "", "")
                password = os.environ.get(_self.password_env_var or "", "")
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {"Authorization": f"Basic {credentials}", "Accept": "application/json"}
                try:
                    resp = requests.get(
                        f"{instance_url}/rest/v1/project/name/{_self.project}/version/name/{_self.version}/job/name/{_self.job_name}/runs",
                        headers=headers,
                        params={"limit": 1},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    runs = resp.json()
                    latest = (runs if isinstance(runs, list) else runs.get("runs", [None]))[0] if runs else None
                except Exception as e:
                    return SensorResult(skip_reason=f"Matillion API error: {e}")

            if not latest:
                return SensorResult(skip_reason="No runs found for this job")

            run_id = str(latest.get("id", ""))
            status = (latest.get("state") or latest.get("status") or "").upper()

            cursor = context.cursor or ""
            if status == "SUCCESS" and run_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "project": _self.project,
                            "job_name": _self.job_name,
                            "run_id": run_id,
                        }}},
                    )],
                    cursor=run_id,
                )

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[matillion_job_sensor])
