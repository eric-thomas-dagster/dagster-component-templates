"""Qlik Replicate Task Status Sensor Component.

Polls the Qlik Enterprise Manager REST API and triggers a Dagster job when a
Replicate task transitions to a target state (default: task completes / stops
after a full-load or reload).
"""
from typing import List

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class QlikReplicateTaskStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Qlik Replicate task hits a target state.

    Common patterns:

    - **Task-completed pattern**: watch a one-shot migration task; trigger a
      downstream validation / transform job when it hits `STOPPED`.
    - **Task-errored pattern**: watch a critical CDC task; trigger an alert
      / remediation job when it hits `ERROR`.

    Example:

        ```yaml
        type: dagster_community_components.QlikReplicateTaskStatusSensorComponent
        attributes:
          sensor_name: orders_migration_done
          server: prod-replicate-01
          task: orders_sqlserver_to_snowflake
          target_states: [STOPPED]
          job_name: validate_orders_target
          resource_key: qlik_replicate_resource
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    server: str = Field(description="Replicate server name as registered in Enterprise Manager.")
    task: str = Field(description="Replicate task name to watch.")
    target_states: List[str] = Field(
        default_factory=lambda: ["STOPPED"],
        description=(
            "Task states that should trigger the downstream job. Common values: "
            "STOPPED, RUNNING, ERROR, STARTING. Match is case-insensitive."
        ),
    )
    job_name: str = Field(description="Job to trigger on state match.")
    resource_key: str = Field(default="qlik_replicate_resource", description="Resource key to look up.")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="running | stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )
        target_upper = {s.upper() for s in self.target_states}

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def qlik_replicate_task_status_sensor(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            resource = getattr(context.resources, _self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            login_body = resource.login_body()
            if login_body is not None:
                try:
                    r = session.post(
                        f"{resource.api_base}/login",
                        json=login_body,
                        headers={"Accept": "application/json", "Content-Type": "application/json"},
                        timeout=30,
                    )
                    if r.status_code >= 300:
                        return SensorResult(skip_reason=f"Qlik EM login failed: {r.status_code}")
                except Exception as e:
                    return SensorResult(skip_reason=f"Qlik EM login error: {e}")

            try:
                resp = session.get(
                    resource.task_url(_self.server, _self.task),
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json() or {}
            except Exception as e:
                return SensorResult(skip_reason=f"Qlik EM task lookup error: {e}")

            task_obj = body.get("task") or body
            state = (task_obj.get("state") or "").upper()
            last_transition = (
                task_obj.get("last_state_change_time")
                or task_obj.get("stop_time")
                or task_obj.get("start_time")
                or ""
            )

            if state not in target_upper:
                return SensorResult(skip_reason=f"Task state {state!r} not in target set {sorted(target_upper)}")

            cursor = context.cursor or ""
            fingerprint = f"{state}|{last_transition}"
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed transition {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={
                            "ops": {
                                "config": {
                                    "qlik_server": _self.server,
                                    "qlik_task": _self.task,
                                    "state": state,
                                    "transition_time": last_transition,
                                }
                            }
                        },
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[qlik_replicate_task_status_sensor])
