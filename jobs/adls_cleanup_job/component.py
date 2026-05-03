"""AdlsCleanupJobComponent.

Delete Azure ADLS Gen2 / Blob objects older than N days under a prefix.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class AdlsCleanupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Delete Azure ADLS Gen2 / Blob objects older than N days under a prefix."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    account_url: str = Field(description="e.g. 'https://myaccount.blob.core.windows.net'")
    container: str = Field(description="Container name")
    prefix: str = Field(default="")
    older_than_days: int = Field(description="Delete blobs older than this many days")
    dry_run: bool = Field(default=False)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
            import datetime as dt
            bsc = BlobServiceClient(account_url=self.account_url, credential=DefaultAzureCredential())
            cc = bsc.get_container_client(self.container)
            cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=self.older_than_days)
            deleted, scanned = 0, 0
            for blob in cc.list_blobs(name_starts_with=self.prefix):
                scanned += 1
                if blob.creation_time and blob.creation_time < cutoff:
                    if not self.dry_run:
                        cc.delete_blob(blob.name)
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
