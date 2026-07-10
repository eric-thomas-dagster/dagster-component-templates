"""TM1ProcessTriggerJobComponent.

Trigger a TM1 TI process or chore via the TM1 REST API. Pair with
`tm1_resource` for centralized auth.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class TM1ProcessTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a TM1 process or chore via the TM1 REST API.

    Target types (via `target_type`):
      - `process` — a single TI process, optionally with parameter overrides
      - `chore` — a group of processes with per-chore scheduling metadata

    Example (nightly data-load process):

        ```yaml
        type: dagster_community_components.TM1ProcessTriggerJobComponent
        attributes:
          job_name: nightly_gl_load
          target_type: process
          target_name: LoadActualsFromGL
          parameters:
            pYear: "2026"
            pMonth: "07"
          resource_key: tm1_resource
          wait_for_completion: true
          schedule: "0 4 * * *"
        ```

    Example (kick off a chore):

        ```yaml
        attributes:
          job_name: refresh_planning_cubes
          target_type: chore
          target_name: DailyRebuild
          resource_key: tm1_resource
        ```
    """

    job_name: str = Field(description="Dagster job name.")
    target_type: str = Field(default="process", description="process | chore")
    target_name: str = Field(description="Name of the TM1 process or chore to execute.")
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional parameter overrides for a process. Keys are TI parameter names (e.g. pYear).",
    )

    wait_for_completion: bool = Field(
        default=True,
        description=(
            "Wait for the process/chore to complete. TM1's ExecuteProcess is synchronous by "
            "default; setting false uses ExecuteProcessWithReturn for fire-and-forget."
        ),
    )
    timeout_seconds: int = Field(default=1800, description="Give up after this many seconds. Raises on timeout.")

    resource_key: str = Field(default="tm1_resource", description="Resource key to look up.")
    schedule: Optional[str] = Field(default=None, description="Optional cron schedule.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags.")

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

            if self.target_type == "process":
                # POST /api/v1/Processes('name')/tm1.ExecuteProcess
                execute_url = f"{resource.process_url(self.target_name)}/tm1.ExecuteProcess"
                body: dict = {}
                if self.parameters:
                    body["Parameters"] = [
                        {"Name": k, "Value": str(v)} for k, v in self.parameters.items()
                    ]
            elif self.target_type == "chore":
                execute_url = f"{resource.chore_url(self.target_name)}/tm1.Execute"
                body = {}
            else:
                raise Exception(f"Unknown target_type={self.target_type!r} (expected process | chore)")

            start = time.time()
            r = session.post(execute_url, json=body, headers=headers, timeout=self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"TM1 execute failed: {r.status_code} {r.text[:200]} "
                    f"(target={self.target_type}:{self.target_name})"
                )
            duration = time.time() - start
            context.log.info(
                f"TM1: {self.target_type} {self.target_name} executed in {duration:.2f}s "
                f"(status={r.status_code})"
            )

            # Some TM1 versions return a ProcessExecuteResult with a status field.
            try:
                body_out = r.json() or {}
                status = body_out.get("ProcessExecuteStatusCode") or body_out.get("Status")
                if status and status not in ("CompletedSuccessfully", "Success"):
                    raise Exception(
                        f"TM1 process ended with status {status}: "
                        f"{body_out.get('ErrorLogFile') or ''}"
                    )
                if status:
                    context.log.info(f"process status: {status}")
            except ValueError:
                pass  # not JSON — bare 200 is fine

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
