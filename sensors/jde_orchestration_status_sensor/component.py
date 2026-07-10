"""JDE Orchestration Status Sensor Component.

Polls JDE Orchestrator status endpoint for a specific jobId or the last
run of an orchestration; triggers a Dagster job on transitions into
target states.
"""
from typing import List, Optional

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class JDEOrchestrationStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a JDE orchestration reaches a target state.

    Example:

        ```yaml
        type: dagster_community_components.JDEOrchestrationStatusSensorComponent
        attributes:
          sensor_name: ar_recon_done
          orchestration: JDE_AR_Recon
          target_states: [SUCCESS, COMPLETED]
          job_name: refresh_finance_dashboards
          resource_key: jde_orchestrator_resource
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    orchestration: str = Field(description="Orchestration name to watch.")
    target_states: List[str] = Field(default_factory=lambda: ["SUCCESS", "COMPLETED"])
    job_name: str = Field(description="Job to trigger.")
    resource_key: str = Field(default="jde_orchestrator_resource")
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
        def jde_orchestration_status_sensor(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            resource = getattr(context.resources, _self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            # Orchestrator exposes last-run status via
            # GET /orchestrator/{name}/history?limit=1
            try:
                url = f"{resource.orchestration_url(_self.orchestration)}/history?limit=1"
                resp = session.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                body = resp.json() or {}
            except Exception as e:
                return SensorResult(skip_reason=f"JDE status lookup error: {e}")

            runs = body.get("history") or body.get("value") or []
            if not runs:
                return SensorResult(skip_reason="No history available yet")

            last = runs[0] if isinstance(runs, list) else runs
            state = (last.get("status") or last.get("state") or "").upper()
            job_id = last.get("jobId") or last.get("id") or ""

            if state not in target_upper:
                return SensorResult(skip_reason=f"State {state!r} not in target set {sorted(target_upper)}")

            cursor = context.cursor or ""
            fingerprint = f"{state}|{job_id}"
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={"ops": {"config": {
                            "jde_orchestration": _self.orchestration,
                            "state": state,
                            "job_id": job_id,
                        }}},
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[jde_orchestration_status_sensor])
