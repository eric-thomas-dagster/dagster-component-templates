"""ShellCommandJobComponent.

Run a shell command as a Dagster job (no asset materialized) — for cleanup, maintenance, ad-hoc tasks.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class ShellCommandJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a shell command as a Dagster job (no asset materialized) — for cleanup, maintenance, ad-hoc tasks."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    command: str = Field(description="Shell command to execute (passed to `bash -c`)")
    working_dir: Optional[str] = Field(default=None, description="cwd for the command")
    env_vars: Optional[dict] = Field(default=None, description="Extra environment variables")
    timeout_seconds: int = Field(default=600, description="Timeout in seconds")
    fail_on_nonzero: bool = Field(default=True, description="Fail the job if exit code != 0")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import subprocess, os
            env = {**os.environ, **(self.env_vars or {})}
            context.log.info(f"running: {self.command}")
            r = subprocess.run(["bash", "-c", self.command], cwd=self.working_dir, env=env,
                               capture_output=True, text=True, timeout=self.timeout_seconds)
            if r.stdout: context.log.info(r.stdout)
            if r.stderr: context.log.warning(r.stderr)
            if self.fail_on_nonzero and r.returncode != 0:
                raise Exception(f"command failed exit={r.returncode}")

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
