"""QlikComposeWorkflowTriggerJobComponent.

Trigger a Qlik Compose workflow via the Compose REST API.
"""
import time
from typing import Optional

import dagster as dg
from pydantic import Field


class QlikComposeWorkflowTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Qlik Compose workflow.

    Example:

        ```yaml
        type: dagster_community_components.QlikComposeWorkflowTriggerJobComponent
        attributes:
          job_name: build_finance_dw
          project: FinanceDW
          workflow: FullBuildAndPopulate
          resource_key: qlik_compose_resource
          wait_for_completion: true
          poll_interval_seconds: 30
          timeout_seconds: 3600
          schedule: "0 2 * * *"
        ```
    """

    job_name: str = Field(description="Dagster job name.")
    project: str = Field(description="Compose project name (Data Warehouse).")
    workflow: str = Field(description="Compose workflow name.")
    action: str = Field(default="run", description="run | stop")
    wait_for_completion: bool = Field(default=True, description="Poll to terminal state.")
    poll_interval_seconds: int = Field(default=30)
    timeout_seconds: int = Field(default=3600)

    resource_key: str = Field(default="qlik_compose_resource")
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
                r = session.post(f"{resource.api_base}/login", json=login_body, timeout=30)
                if r.status_code >= 300:
                    raise Exception(f"Compose login failed: {r.status_code} {r.text[:200]}")

            workflow_url = resource.workflow_url(self.project, self.workflow)
            r = session.post(f"{workflow_url}?action={self.action}", headers=headers, timeout=60)
            if r.status_code >= 300:
                raise Exception(
                    f"Compose action failed: {r.status_code} {r.text[:200]} "
                    f"(project={self.project} workflow={self.workflow} action={self.action})"
                )
            context.log.info(f"Qlik Compose: {self.action} sent to {self.project}/{self.workflow}")

            if self.wait_for_completion:
                deadline = time.time() + self.timeout_seconds
                terminal = {"COMPLETED", "STOPPED", "ERROR", "FAILED"}
                last_state = None
                while time.time() < deadline:
                    time.sleep(self.poll_interval_seconds)
                    sr = session.get(workflow_url, headers=headers, timeout=30)
                    if sr.status_code >= 300:
                        continue
                    body = sr.json() or {}
                    state = (body.get("workflow", {}) or {}).get("state") or body.get("state") or ""
                    if state and state != last_state:
                        context.log.info(f"workflow state: {state}")
                        last_state = state
                    if state and state.upper() in terminal:
                        if state.upper() in ("ERROR", "FAILED"):
                            raise Exception(
                                f"Compose workflow ended in {state} (project={self.project}, workflow={self.workflow})"
                            )
                        return
                raise Exception(
                    f"Workflow did not reach terminal state within {self.timeout_seconds}s "
                    f"(last state={last_state}, project={self.project}, workflow={self.workflow})"
                )

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
