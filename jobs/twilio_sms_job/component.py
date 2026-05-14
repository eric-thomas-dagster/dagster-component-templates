"""TwilioSmsJobComponent.

Send an SMS via Twilio as a job — useful for on-call paging without a full PagerDuty stack.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class TwilioSmsJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send an SMS via Twilio as a job — useful for on-call paging without a full PagerDuty stack."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    account_sid_env: str = Field(default="TWILIO_ACCOUNT_SID")
    auth_token_env: str = Field(default="TWILIO_AUTH_TOKEN")
    from_number: str = Field(description="Twilio source phone number (E.164, e.g. '+15551234567')")
    to_numbers: list = Field(description="List of destination numbers (E.164)")
    message: str = Field(description="SMS text body")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            from twilio.rest import Client
            import os
            client = Client(os.environ[self.account_sid_env], os.environ[self.auth_token_env])
            sent = []
            for to in self.to_numbers:
                msg = client.messages.create(body=self.message, from_=self.from_number, to=to)
                sent.append(msg.sid)
                context.log.info(f"sent to {to} sid={msg.sid}")
            context.log.info(f"{len(sent)} messages delivered")

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
