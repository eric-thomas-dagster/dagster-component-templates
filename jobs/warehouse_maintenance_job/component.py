"""WarehouseMaintenanceJobComponent.

Compound op job: run warehouse maintenance (VACUUM / ANALYZE / REFRESH MATERIALIZED VIEW) across many statements with one schedule.

Compound op-job — wires multiple ops + an optional schedule from one YAML.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class WarehouseMaintenanceJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Compound op job: run warehouse maintenance (VACUUM / ANALYZE / REFRESH MATERIALIZED VIEW) across many statements with one schedule."""


    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule")
    default_status: str = Field(default="STOPPED")
    connection_string_env: str = Field(default="DATABASE_URL")
    statements: list = Field(description="List of SQL statements run sequentially as ops")
    autocommit: bool = Field(default=True, description="Required for VACUUM on Postgres")
    fail_fast: bool = Field(default=True, description="Stop on first failure (False = collect errors and continue)")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from sqlalchemy import create_engine, text
        import os
        _self = self

        @dg.op
        def _maint(_ctx):
            url = os.environ[_self.connection_string_env]
            engine = create_engine(url, isolation_level="AUTOCOMMIT" if _self.autocommit else None)
            errors = []
            with engine.connect() as conn:
                for i, stmt in enumerate(_self.statements):
                    _ctx.log.info(f"[{i+1}/{len(_self.statements)}] {stmt[:120]}...")
                    try:
                        conn.execute(text(stmt))
                    except Exception as exc:
                        msg = f"stmt {i+1} failed: {exc}"
                        _ctx.log.error(msg)
                        errors.append(msg)
                        if _self.fail_fast:
                            raise
            if errors and not _self.fail_fast:
                raise Exception(f"{len(errors)}/{len(_self.statements)} statements failed: {errors[:3]}")

        @dg.job(name=self.job_name, tags={"compound": "warehouse_maintenance"})
        def _the_job():
            _maint()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
