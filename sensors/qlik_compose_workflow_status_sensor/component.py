"""Qlik Compose Workflow Status Sensor Component.

Polls the Compose REST API and triggers a Dagster job when a workflow
transitions to a target state.
"""
from typing import List

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class QlikComposeWorkflowStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Compose workflow hits a target state.

    Example:

        ```yaml
        type: dagster_community_components.QlikComposeWorkflowStatusSensorComponent
        attributes:
          sensor_name: finance_dw_done
          project: FinanceDW
          workflow: FullBuildAndPopulate
          target_states: [COMPLETED]
          job_name: refresh_downstream_marts
          resource_key: qlik_compose_resource
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    project: str = Field(description="Compose project.")
    workflow: str = Field(description="Compose workflow.")
    target_states: List[str] = Field(default_factory=lambda: ["COMPLETED"])
    job_name: str = Field(description="Job to trigger.")
    resource_key: str = Field(default="qlik_compose_resource")
    minimum_interval_seconds: int = Field(default=60)
    default_status: str = Field(default="running")

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
        def qlik_compose_workflow_status_sensor(context: SensorEvaluationContext):
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
                    r = session.post(f"{resource.api_base}/login", json=login_body, timeout=30)
                    if r.status_code >= 300:
                        return SensorResult(skip_reason=f"Compose login failed: {r.status_code}")
                except Exception as e:
                    return SensorResult(skip_reason=f"Compose login error: {e}")

            try:
                resp = session.get(resource.workflow_url(_self.project, _self.workflow), headers=headers, timeout=30)
                resp.raise_for_status()
                body = resp.json() or {}
            except Exception as e:
                return SensorResult(skip_reason=f"Compose workflow lookup error: {e}")

            wf = body.get("workflow") or body
            state = (wf.get("state") or "").upper()
            last_completion = wf.get("last_completion_time") or wf.get("last_run_time") or ""

            if state not in target_upper:
                return SensorResult(skip_reason=f"State {state!r} not in target set {sorted(target_upper)}")

            cursor = context.cursor or ""
            fingerprint = f"{state}|{last_completion}"
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={
                            "ops": {
                                "config": {
                                    "qlik_project": _self.project,
                                    "qlik_workflow": _self.workflow,
                                    "state": state,
                                    "completion_time": last_completion,
                                }
                            }
                        },
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[qlik_compose_workflow_status_sensor])
