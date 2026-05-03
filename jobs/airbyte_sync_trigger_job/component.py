"""AirbyteSyncTriggerJobComponent.

Trigger an Airbyte connection sync via REST API — fire-and-forget or wait for completion.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class AirbyteSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Airbyte connection sync via REST API — fire-and-forget or wait for completion."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    airbyte_url: str = Field(description="Airbyte server URL (e.g. 'https://airbyte.acme.com')")
    connection_id: str = Field(description="Connection UUID to sync")
    username_env: Optional[str] = Field(default="AIRBYTE_USER")
    password_env: Optional[str] = Field(default="AIRBYTE_PASSWORD")
    wait_for_completion: bool = Field(default=False)
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=3600)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os, time
            auth = (os.environ[self.username_env], os.environ[self.password_env]) if self.username_env else None
            r = requests.post(f"{self.airbyte_url}/api/v1/connections/sync",
                              json={"connectionId": self.connection_id}, auth=auth, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"airbyte sync failed: {r.status_code} {r.text[:200]}")
            job_id = r.json().get("job", {}).get("id")
            context.log.info(f"started job {job_id}")
            if self.wait_for_completion and job_id:
                deadline = time.time() + self.timeout_seconds
                while time.time() < deadline:
                    jr = requests.post(f"{self.airbyte_url}/api/v1/jobs/get",
                                       json={"id": job_id}, auth=auth, timeout=30)
                    status = jr.json().get("job", {}).get("status")
                    context.log.info(f"poll: status={status}")
                    if status in ("succeeded", "failed", "cancelled"):
                        if status != "succeeded":
                            raise Exception(f"airbyte job {job_id} {status}")
                        return
                    time.sleep(self.poll_interval_seconds)
                raise Exception(f"timed out waiting for {job_id}")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
