"""dbt Cloud Job Sensor Component.

Polls the dbt Cloud Admin API and triggers a Dagster job when a specified
dbt Cloud job run completes successfully.

resource_key, when set, genuinely drives the poll via a registered
DbtCloudResourceComponent's own (real, dagster-dbt) DbtCloudResource.get_runs(...)
method -- previously this field was declared and wired into
required_resource_keys but never actually read, so setting it silently had
no effect and the sensor always fell back to its own inline requests call
using account_id/api_token_env_var regardless.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field, model_validator


class DbtCloudJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a dbt Cloud job run completes.

    Example:
        ```yaml
        type: dagster_community_components.DbtCloudJobSensorComponent
        attributes:
          sensor_name: dbt_daily_models_done
          account_id: 12345
          job_id: 67890
          api_token_env_var: DBT_CLOUD_API_TOKEN
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    account_id: Optional[int] = Field(default=None, description="dbt Cloud account ID. Required unless resource_key is set.")
    job_id: int = Field(description="dbt Cloud job ID to monitor")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var containing the dbt Cloud API token. Required unless resource_key is set.")
    job_name: str = Field(description="Dagster job to trigger when dbt run completes")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional resource key registered by a DbtCloudResourceComponent. When set, polls "
            "run via that resource's own get_runs(...) method instead of the inline "
            "account_id/api_token_env_var below."
        ),
    )

    @model_validator(mode="after")
    def _check_credentials(self):
        if not self.resource_key and not (self.account_id and self.api_token_env_var):
            raise ValueError(
                "DbtCloudJobSensorComponent: supply resource_key (a registered "
                "DbtCloudResourceComponent) OR both account_id and api_token_env_var."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dbt Cloud Job Sensor', editable=True)

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
        def dbt_cloud_sensor(context: SensorEvaluationContext, **_resources):
            if resource_key:
                _res = getattr(context.resources, resource_key, None)
                if _res is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    runs = _res.get_runs(job_id=_self.job_id, order_by="-id", limit=1)
                except Exception as e:
                    return SensorResult(skip_reason=f"dbt Cloud API error (via resource '{resource_key}'): {e}")
            else:
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
