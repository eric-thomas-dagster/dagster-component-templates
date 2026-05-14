"""SlackNotificationJobComponent.

Send a Slack message as a scheduled job — daily summaries, weekly reports, ad-hoc alerts.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class SlackNotificationJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a Slack message as a scheduled job — daily summaries, weekly reports, ad-hoc alerts."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    webhook_url_env: str = Field(default="SLACK_WEBHOOK_URL", description="Env var with Slack incoming-webhook URL")
    message: str = Field(description="Message text (supports Slack markdown)")
    channel: Optional[str] = Field(default=None, description="Override channel (e.g. '#data-platform')")
    username: Optional[str] = Field(default=None)
    icon_emoji: Optional[str] = Field(default=":robot_face:")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os
            url = os.environ[self.webhook_url_env]
            payload = {"text": self.message}
            if self.channel: payload["channel"] = self.channel
            if self.username: payload["username"] = self.username
            if self.icon_emoji: payload["icon_emoji"] = self.icon_emoji
            r = requests.post(url, json=payload, timeout=30)
            if r.status_code >= 300:
                raise Exception(f"slack webhook failed: {r.status_code} {r.text[:200]}")
            context.log.info(f"sent {len(self.message)} chars to slack")

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
