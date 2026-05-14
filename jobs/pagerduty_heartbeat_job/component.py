"""PagerdutyHeartbeatJobComponent.

Send a PagerDuty heartbeat (resolve event) — confirms a job is alive on a cadence.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class PagerdutyHeartbeatJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a PagerDuty heartbeat (resolve event) — confirms a job is alive on a cadence."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    routing_key_env: str = Field(default="PAGERDUTY_ROUTING_KEY", description="Env var with PD Events API integration key")
    summary: str = Field(default="Dagster job heartbeat")
    source: str = Field(default="dagster", description="`source` field for the event")
    severity: str = Field(default="info", description="critical | error | warning | info")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os
            key = os.environ[self.routing_key_env]
            payload = {
                "routing_key": key, "event_action": "resolve", "dedup_key": f"dagster-heartbeat-{self.source}",
                "payload": {"summary": self.summary, "source": self.source, "severity": self.severity},
            }
            r = requests.post("https://events.pagerduty.com/v2/enqueue", json=payload, timeout=30)
            if r.status_code >= 300:
                raise Exception(f"pagerduty failed: {r.status_code} {r.text[:200]}")
            context.log.info("heartbeat sent")

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
