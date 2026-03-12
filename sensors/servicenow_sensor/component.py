"""ServiceNow Sensor Component.

Polls the ServiceNow Table API and triggers a Dagster job when records
in a specified table reach a target state (e.g., a change ticket is approved).
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class ServiceNowSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a job when ServiceNow records match a target state.

    Example:
        ```yaml
        type: dagster_component_templates.ServiceNowSensorComponent
        attributes:
          sensor_name: snow_change_approved_sensor
          instance_env_var: SNOW_INSTANCE
          username_env_var: SNOW_USERNAME
          password_env_var: SNOW_PASSWORD
          table: change_request
          sysparm_query: "state=approved^assignment_group=data_engineering"
          job_name: execute_approved_change_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    instance_env_var: str = Field(description="Env var with ServiceNow instance name (e.g. mycompany)")
    username_env_var: str = Field(description="Env var with ServiceNow username")
    password_env_var: str = Field(description="Env var with ServiceNow password")
    table: str = Field(description="ServiceNow table to query (e.g. change_request, incident)")
    sysparm_query: str = Field(description="ServiceNow encoded query string to filter records")
    job_name: str = Field(description="Job to trigger when matching records are found")
    sysparm_fields: str = Field(
        default="sys_id,number,state,short_description,sys_updated_on",
        description="Comma-separated fields to return",
    )
    minimum_interval_seconds: int = Field(default=120, description="Seconds between polls")
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
        def snow_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            instance = os.environ.get(_self.instance_env_var, "")
            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var, "")

            cursor = context.cursor or ""
            # Append sys_updated_on > cursor to the query if we have a cursor
            query = _self.sysparm_query
            if cursor:
                query += f"^sys_updated_on>{cursor}"

            try:
                resp = requests.get(
                    f"https://{instance}.service-now.com/api/now/table/{_self.table}",
                    auth=(username, password),
                    params={
                        "sysparm_query": query,
                        "sysparm_fields": _self.sysparm_fields,
                        "sysparm_limit": 100,
                        "sysparm_order_by": "sys_updated_on",
                    },
                    headers={"Accept": "application/json"},
                    timeout=30,
                )
                resp.raise_for_status()
                records = resp.json().get("result", [])
            except Exception as e:
                return SensorResult(skip_reason=f"ServiceNow API error: {e}")

            if not records:
                return SensorResult(skip_reason="No matching records found")

            latest_updated = cursor
            run_requests = []
            for record in records:
                sys_id = record.get("sys_id", "")
                updated = record.get("sys_updated_on", "")
                run_requests.append(RunRequest(
                    run_key=f"{sys_id}-{updated}",
                    run_config={"ops": {"config": {
                        "table": _self.table,
                        "sys_id": sys_id,
                        "number": record.get("number", ""),
                        "state": record.get("state", ""),
                        "short_description": record.get("short_description", ""),
                        "sys_updated_on": updated,
                    }}},
                ))
                if updated > latest_updated:
                    latest_updated = updated

            return SensorResult(run_requests=run_requests, cursor=latest_updated)

        return dg.Definitions(sensors=[snow_sensor])
