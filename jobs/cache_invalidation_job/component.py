"""CacheInvalidationJobComponent.

Flush keys from a Redis (or compatible) cache as a job — pattern-based or full FLUSHDB.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class CacheInvalidationJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Flush keys from a Redis (or compatible) cache as a job — pattern-based or full FLUSHDB."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    redis_url_env: str = Field(default="REDIS_URL", description="redis://[user:pass@]host:port[/db]")
    pattern: Optional[str] = Field(default=None, description="Key glob pattern (e.g. 'session:*'). If None, FLUSHDB.")
    db: int = Field(default=0)
    batch_size: int = Field(default=500)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import redis, os
            client = redis.from_url(os.environ[self.redis_url_env], db=self.db)
            if self.pattern:
                deleted = 0
                cursor, batch = 0, []
                while True:
                    cursor, keys = client.scan(cursor=cursor, match=self.pattern, count=self.batch_size)
                    if keys:
                        client.delete(*keys)
                        deleted += len(keys)
                    if cursor == 0:
                        break
                context.log.info(f"deleted {deleted} keys matching {self.pattern}")
            else:
                client.flushdb()
                context.log.info(f"flushed db {self.db}")

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
