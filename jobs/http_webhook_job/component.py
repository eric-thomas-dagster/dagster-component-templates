"""HttpWebhookJobComponent.

POST/GET to an HTTP endpoint as a job — heartbeats, status pings, fire-and-forget triggers.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class HttpWebhookJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """POST/GET to an HTTP endpoint as a job — heartbeats, status pings, fire-and-forget triggers."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    url: str = Field(description="URL to call")
    method: str = Field(default="POST", description="HTTP method")
    body: Optional[dict] = Field(default=None, description="JSON body (POST/PUT)")
    headers: Optional[dict] = Field(default=None, description="HTTP headers")
    expected_status: int = Field(default=200, description="Fail unless this exact status code is returned")
    timeout_seconds: int = Field(default=30)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests
            context.log.info(f"{self.method} {self.url}")
            r = requests.request(self.method, self.url, json=self.body, headers=self.headers, timeout=self.timeout_seconds)
            context.log.info(f"got {r.status_code}")
            if r.status_code != self.expected_status:
                raise Exception(f"expected {self.expected_status}, got {r.status_code}: {r.text[:200]}")

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
