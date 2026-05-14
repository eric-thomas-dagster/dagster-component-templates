"""TeamsNotificationJobComponent.

Send a message to a Microsoft Teams channel via incoming webhook.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class TeamsNotificationJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a message to a Microsoft Teams channel via incoming webhook."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    webhook_url_env: str = Field(default="TEAMS_WEBHOOK_URL", description="Env var with Teams incoming-webhook URL")
    title: str = Field(description="Message title")
    text: str = Field(description="Message body (Markdown)")
    theme_color: str = Field(default="0076D7", description="Hex color for the card stripe")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os
            url = os.environ[self.webhook_url_env]
            card = {
                "@type": "MessageCard", "@context": "https://schema.org/extensions",
                "themeColor": self.theme_color, "title": self.title, "text": self.text,
            }
            r = requests.post(url, json=card, timeout=30)
            if r.status_code >= 300:
                raise Exception(f"teams webhook failed: {r.status_code} {r.text[:200]}")
            context.log.info("teams message sent")

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
