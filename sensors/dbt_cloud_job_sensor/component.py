"""dbt Cloud Job Sensor Component.

Polls the dbt Cloud Admin API and triggers a Dagster job when a specified
dbt Cloud job run completes successfully.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class DbtCloudJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a dbt Cloud job run completes.

    Example:
        ```yaml
        type: dagster_component_templates.DbtCloudJobSensorComponent
        attributes:
          sensor_name: dbt_daily_models_done
          account_id: 12345
          job_id: 67890
          api_token_env_var: DBT_CLOUD_API_TOKEN
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    account_id: int = Field(description="dbt Cloud account ID")
    job_id: int = Field(description="dbt Cloud job ID to monitor")
    api_token_env_var: str = Field(description="Env var containing the dbt Cloud API token")
    job_name: str = Field(description="Dagster job to trigger when dbt run completes")
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
        def dbt_cloud_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            token = os.environ.get(_self.api_token_env_var, "")
            headers = {"Authorization": f"Token {token}"}
            base = f"https://cloud.getdbt.com/api/v2/accounts/{_self.account_id}"

            try:
                resp = requests.get(
                    f"{base}/runs/",
                    headers=headers,
                    params={"job_definition_id": _self.job_id, "order_by": "-id", "limit": 1},
                    timeout=30,
                )
                resp.raise_for_status()
                runs = resp.json().get("data", [])
            except Exception as e:
                return SensorResult(skip_reason=f"dbt Cloud API error: {e}")

            if not runs:
                return SensorResult(skip_reason="No runs found for this job")

            run = runs[0]
            run_id = str(run["id"])
            status = run.get("status")  # 1=queued, 2=starting, 3=running, 10=success, 20=error, 30=cancelled
            finished_at = run.get("finished_at", "")

            cursor = context.cursor or ""
            if status == 10 and run_id != cursor:  # 10 = success
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "dbt_job_id": _self.job_id,
                            "dbt_run_id": int(run_id),
                            "finished_at": finished_at,
                        }}},
                    )],
                    cursor=run_id,
                )

            status_labels = {1:"queued",2:"starting",3:"running",10:"success",20:"error",30:"cancelled"}
            return SensorResult(skip_reason=f"Run {run_id} status: {status_labels.get(status, status)}")

        return dg.Definitions(sensors=[dbt_cloud_sensor])
