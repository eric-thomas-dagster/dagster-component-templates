"""Fivetran Sync Sensor Component.

Polls the Fivetran REST API and triggers a Dagster job when a specified
connector's sync completes successfully.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class FivetranSyncSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a job when a Fivetran connector sync completes.

    Example:
        ```yaml
        type: dagster_component_templates.FivetranSyncSensorComponent
        attributes:
          sensor_name: fivetran_orders_sync_done
          connector_id: my_connector_id
          api_key_env_var: FIVETRAN_API_KEY
          api_secret_env_var: FIVETRAN_API_SECRET
          job_name: process_synced_orders_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    connector_id: str = Field(description="Fivetran connector ID")
    api_key_env_var: str = Field(description="Env var containing the Fivetran API key")
    api_secret_env_var: str = Field(description="Env var containing the Fivetran API secret")
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
        def fivetran_sync_sensor(context: SensorEvaluationContext):
            import os, base64
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            api_key = os.environ.get(_self.api_key_env_var, "")
            api_secret = os.environ.get(_self.api_secret_env_var, "")
            credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()

            try:
                resp = requests.get(
                    f"https://api.fivetran.com/v1/connectors/{_self.connector_id}",
                    headers={"Authorization": f"Basic {credentials}"},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()["data"]
            except Exception as e:
                return SensorResult(skip_reason=f"Fivetran API error: {e}")

            status = data.get("status", {})
            sync_state = status.get("sync_state", "")
            last_sync = data.get("succeeded_at") or data.get("status", {}).get("last_sync")

            # Cursor tracks the last succeeded_at we processed
            cursor = context.cursor or ""
            if sync_state == "scheduled" and last_sync and last_sync != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=last_sync,
                        run_config={"ops": {"config": {
                            "connector_id": _self.connector_id,
                            "succeeded_at": last_sync,
                        }}},
                    )],
                    cursor=last_sync,
                )

            return SensorResult(skip_reason=f"No new sync. State: {sync_state}, last: {last_sync}")

        return dg.Definitions(sensors=[fivetran_sync_sensor])
