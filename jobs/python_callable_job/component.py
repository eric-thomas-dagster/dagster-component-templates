"""PythonCallableJobComponent.

Run an arbitrary Python callable as a Dagster job — Airflow PythonOperator equivalent.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class PythonCallableJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an arbitrary Python callable as a Dagster job — Airflow PythonOperator equivalent."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    callable_path: str = Field(description="'module.path:function_name' (e.g. 'mypkg.cleanup:run')")
    kwargs: Optional[dict] = Field(default=None, description="Keyword arguments passed to the callable")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import importlib
            mod_path, fn_name = self.callable_path.split(":")
            mod = importlib.import_module(mod_path)
            fn = getattr(mod, fn_name)
            context.log.info(f"calling {self.callable_path}({self.kwargs or {}})")
            result = fn(**(self.kwargs or {}))
            context.log.info(f"returned: {result!r}"[:500])

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
