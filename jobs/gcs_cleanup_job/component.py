"""GcsCleanupJobComponent.

Delete GCS objects older than N days under a prefix.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class GcsCleanupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Delete GCS objects older than N days under a prefix."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    bucket: str = Field(description="GCS bucket name")
    prefix: str = Field(default="")
    older_than_days: int = Field(description="Delete objects older than this many days")
    dry_run: bool = Field(default=False)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            from google.cloud import storage
            import datetime as dt
            client = storage.Client()
            bucket = client.bucket(self.bucket)
            cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=self.older_than_days)
            deleted, scanned = 0, 0
            for blob in bucket.list_blobs(prefix=self.prefix):
                scanned += 1
                if blob.time_created and blob.time_created < cutoff:
                    if not self.dry_run:
                        blob.delete()
                    deleted += 1
            context.log.info(f"scanned {scanned}, {'would delete' if self.dry_run else 'deleted'} {deleted}")

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
