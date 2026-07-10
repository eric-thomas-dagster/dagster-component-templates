"""Cognos Report Status Sensor Component.

Polls Cognos for a report's last-run status; triggers a Dagster job on
transitions into target statuses (COMPLETED / FAILED / etc).
"""
from typing import List

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class CognosReportStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Cognos report reaches a target status.

    Example:

        ```yaml
        type: dagster_community_components.CognosReportStatusSensorComponent
        attributes:
          sensor_name: finance_report_done
          report_id: i8B1A56A56789ABCDEF01234567890AB
          target_statuses: [COMPLETED, SUCCEEDED]
          job_name: notify_finance_team
          resource_key: cognos_resource
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    report_id: str = Field(description="Cognos report ID to watch.")
    target_statuses: List[str] = Field(default_factory=lambda: ["COMPLETED", "SUCCEEDED"])
    job_name: str = Field(description="Job to trigger.")
    resource_key: str = Field(default="cognos_resource")
    minimum_interval_seconds: int = Field(default=60)
    default_status: str = Field(default="running")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )
        target_upper = {s.upper() for s in self.target_statuses}

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def cognos_report_status_sensor(context: SensorEvaluationContext):
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
                    r = session.post(f"{resource.api_base}/session", json=login_body, timeout=30)
                    if r.status_code >= 300:
                        return SensorResult(skip_reason=f"Cognos login failed: {r.status_code}")
                except Exception as e:
                    return SensorResult(skip_reason=f"Cognos login error: {e}")

            # GET /reports/{id}/history?limit=1
            try:
                resp = session.get(f"{resource.report_url(_self.report_id)}/history?limit=1", headers=headers, timeout=30)
                resp.raise_for_status()
                body = resp.json() or {}
            except Exception as e:
                return SensorResult(skip_reason=f"Cognos history error: {e}")

            runs = body.get("history") or body.get("value") or []
            if not runs:
                return SensorResult(skip_reason="No history available yet")

            last = runs[0]
            status = (last.get("status") or "").upper()
            run_id = last.get("id") or last.get("runId") or ""

            if status not in target_upper:
                return SensorResult(skip_reason=f"Status {status!r} not in target set {sorted(target_upper)}")

            cursor = context.cursor or ""
            fingerprint = f"{status}|{run_id}"
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={"ops": {"config": {
                            "cognos_report_id": _self.report_id,
                            "status": status,
                            "run_id": run_id,
                        }}},
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[cognos_report_status_sensor])
