"""SqlCommandJobComponent.

Execute a SQL maintenance command (VACUUM, ANALYZE, REFRESH MATERIALIZED VIEW, etc.) — no asset.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class SqlCommandJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute a SQL maintenance command (VACUUM, ANALYZE, REFRESH MATERIALIZED VIEW, etc.) — no asset."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    connection_string_env: str = Field(default="DATABASE_URL", description="Env var with SQLAlchemy URL")
    sql: str = Field(description="SQL statement to execute (one or many, separated by semicolons)")
    autocommit: bool = Field(default=True, description="Required for VACUUM, certain DDL on Postgres")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            from sqlalchemy import create_engine, text
            import os
            url = os.environ[self.connection_string_env]
            engine = create_engine(url, isolation_level="AUTOCOMMIT" if self.autocommit else None)
            with engine.connect() as conn:
                for stmt in [s.strip() for s in self.sql.split(";") if s.strip()]:
                    context.log.info(f"executing: {stmt[:120]}...")
                    conn.execute(text(stmt))
            context.log.info("sql complete")

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
