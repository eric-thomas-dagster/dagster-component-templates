"""ObservabilityHeartbeatJobComponent.

Compound op job: send a heartbeat to multiple destinations (Slack + PagerDuty + HTTP) on cron — single YAML config.

Compound op-job — wires multiple ops + an optional schedule from one YAML.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class ObservabilityHeartbeatJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Compound op job: send a heartbeat to multiple destinations (Slack + PagerDuty + HTTP) on cron — single YAML config."""


    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule")
    default_status: str = Field(default="STOPPED")
    message: str = Field(default="dagster heartbeat", description="Common message body")

    # Each target is optional — set the env var to enable that channel
    slack_webhook_url_env: Optional[str] = Field(default=None, description="Slack webhook URL env var")
    pagerduty_routing_key_env: Optional[str] = Field(default=None, description="PD routing key env var")
    http_heartbeat_url: Optional[str] = Field(default=None, description="GET this URL (e.g. UptimeRobot)")
    teams_webhook_url_env: Optional[str] = Field(default=None, description="Teams webhook URL env var")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import requests, os
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _heartbeat(context):
            sent = []
            if _self.slack_webhook_url_env:
                u = os.environ[_self.slack_webhook_url_env]
                requests.post(u, json={"text": _self.message}, timeout=30)
                sent.append("slack")
            if _self.pagerduty_routing_key_env:
                k = os.environ[_self.pagerduty_routing_key_env]
                requests.post("https://events.pagerduty.com/v2/enqueue", timeout=30, json={
                    "routing_key": k, "event_action": "resolve",
                    "dedup_key": f"dagster-{_self.job_name}",
                    "payload": {"summary": _self.message, "source": "dagster", "severity": "info"},
                })
                sent.append("pagerduty")
            if _self.http_heartbeat_url:
                requests.get(_self.http_heartbeat_url, timeout=30)
                sent.append("http")
            if _self.teams_webhook_url_env:
                u = os.environ[_self.teams_webhook_url_env]
                requests.post(u, json={"@type": "MessageCard", "title": "Heartbeat", "text": _self.message}, timeout=30)
                sent.append("teams")
            context.log.info(f"heartbeat -> {', '.join(sent) or 'no targets configured'}")

        @dg.job(name=self.job_name, tags={"compound": "heartbeat"})
        def _the_job():
            _heartbeat()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
