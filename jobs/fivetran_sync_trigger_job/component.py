"""FivetranSyncTriggerJobComponent.

Trigger a Fivetran connector sync via REST API.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class FivetranSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Fivetran connector sync via REST API."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    api_key_env: str = Field(default="FIVETRAN_API_KEY")
    api_secret_env: str = Field(default="FIVETRAN_API_SECRET")
    connector_id: str = Field(description="Fivetran connector ID")
    force: bool = Field(default=False, description="Sync even if no changes detected")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os
            auth = (os.environ[self.api_key_env], os.environ[self.api_secret_env])
            url = f"https://api.fivetran.com/v1/connectors/{self.connector_id}/force"
            r = requests.post(url, json={"force": self.force}, auth=auth, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"fivetran trigger failed: {r.status_code} {r.text[:200]}")
            context.log.info(f"triggered {self.connector_id}")

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
