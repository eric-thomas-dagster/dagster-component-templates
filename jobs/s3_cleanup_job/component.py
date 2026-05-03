"""S3CleanupJobComponent.

Delete S3 objects older than N days under a prefix — cost control + compliance retention.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class S3CleanupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Delete S3 objects older than N days under a prefix — cost control + compliance retention."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Object prefix to scope cleanup")
    older_than_days: int = Field(description="Delete objects older than this many days")
    region_name: str = Field(default="us-east-1")
    aws_profile: Optional[str] = Field(default=None)
    dry_run: bool = Field(default=False, description="Report what would be deleted without deleting")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import boto3, datetime as dt
            session = boto3.Session(profile_name=self.aws_profile, region_name=self.region_name)
            s3 = session.client("s3")
            cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=self.older_than_days)
            deleted, scanned = 0, 0
            paginator = s3.get_paginator("list_objects_v2")
            batch = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                for obj in page.get("Contents", []):
                    scanned += 1
                    if obj["LastModified"] < cutoff:
                        batch.append({"Key": obj["Key"]})
                        if len(batch) >= 1000:
                            if not self.dry_run:
                                s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
                            deleted += len(batch)
                            batch = []
            if batch:
                if not self.dry_run:
                    s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
                deleted += len(batch)
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
