"""CognosReportRunJobComponent.

Trigger a Cognos Analytics report execution via REST.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class CognosReportRunJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Cognos report run.

    Example:

        ```yaml
        type: dagster_community_components.CognosReportRunJobComponent
        attributes:
          job_name: daily_finance_report
          report_id: i8B1A56A56789ABCDEF01234567890AB
          output_format: PDF
          parameters:
            AsOfDate: "2026-07-10"
          resource_key: cognos_resource
          wait_for_completion: true
          timeout_seconds: 600
          schedule: "0 6 * * *"
        ```
    """

    job_name: str = Field(description="Dagster job name.")
    report_id: str = Field(description="Cognos report ID (from the report's URL or Store ID).")
    output_format: str = Field(default="PDF", description="PDF | HTML | CSV | XLSX | XML | JSON")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Report parameter overrides.")

    wait_for_completion: bool = Field(default=True)
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=600)

    resource_key: str = Field(default="cognos_resource")
    schedule: Optional[str] = Field(default=None)
    default_status: str = Field(default="STOPPED")
    tags: Optional[dict] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.op(name=f"{self.job_name}_op", required_resource_keys=required_resource_keys)
        def _the_op(context: dg.OpExecutionContext):
            self = _self
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            resource = getattr(context.resources, self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            login_body = resource.login_body()
            if login_body is not None:
                r = session.post(f"{resource.api_base}/session", json=login_body, timeout=30)
                if r.status_code >= 300:
                    raise Exception(f"Cognos login failed: {r.status_code} {r.text[:200]}")

            body: dict = {"format": self.output_format}
            if self.parameters:
                body["parameters"] = [{"name": k, "value": str(v)} for k, v in self.parameters.items()]

            run_url = f"{resource.report_url(self.report_id)}/data"
            r = session.post(run_url, json=body, headers=headers, timeout=self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"Cognos report run failed: {r.status_code} {r.text[:200]} (report={self.report_id})"
                )
            context.log.info(f"Cognos: report {self.report_id} submitted (status={r.status_code})")

            if not self.wait_for_completion:
                return

            try:
                body_out = r.json() or {}
                job_id = body_out.get("id") or body_out.get("jobId")
            except ValueError:
                job_id = None
            if not job_id:
                # Sync mode already completed.
                return

            deadline = time.time() + self.timeout_seconds
            terminal = {"COMPLETED", "SUCCEEDED", "FAILED", "ERROR", "CANCELLED"}
            last_state = None
            while time.time() < deadline:
                time.sleep(self.poll_interval_seconds)
                sr = session.get(f"{resource.api_base}/reports/status/{job_id}", headers=headers, timeout=30)
                if sr.status_code >= 300:
                    continue
                state = ((sr.json() or {}).get("status") or "").upper()
                if state and state != last_state:
                    context.log.info(f"report state: {state}")
                    last_state = state
                if state in terminal:
                    if state in ("FAILED", "ERROR", "CANCELLED"):
                        raise Exception(f"Cognos report ended in {state} (report={self.report_id})")
                    return
            raise Exception(f"Report did not reach terminal state within {self.timeout_seconds}s (report={self.report_id})")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs: dict = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
