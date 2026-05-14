"""DbtRunJobComponent.

Run a `dbt` command (run, build, test, snapshot) as a Dagster job — no asset modeling.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class DbtRunJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a `dbt` command (run, build, test, snapshot) as a Dagster job — no asset modeling."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    project_dir: str = Field(description="Path to dbt project root (where dbt_project.yml lives)")
    profiles_dir: Optional[str] = Field(default=None, description="Path to profiles.yml (default: ~/.dbt)")
    target: Optional[str] = Field(default=None, description="dbt target name (overrides default)")
    command: str = Field(default="run", description="dbt subcommand: run | build | test | snapshot | seed | source")
    select: Optional[str] = Field(default=None, description="--select expression")
    exclude: Optional[str] = Field(default=None, description="--exclude expression")
    full_refresh: bool = Field(default=False)
    vars: Optional[dict] = Field(default=None, description="--vars passed as JSON")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import subprocess, json as _json
            cmd = ["dbt", self.command, "--project-dir", self.project_dir]
            if self.profiles_dir: cmd += ["--profiles-dir", self.profiles_dir]
            if self.target: cmd += ["--target", self.target]
            if self.select: cmd += ["--select", self.select]
            if self.exclude: cmd += ["--exclude", self.exclude]
            if self.full_refresh: cmd.append("--full-refresh")
            if self.vars: cmd += ["--vars", _json.dumps(self.vars)]
            context.log.info(" ".join(cmd))
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.stdout: context.log.info(r.stdout)
            if r.stderr: context.log.warning(r.stderr)
            if r.returncode != 0:
                raise Exception(f"dbt {self.command} failed exit={r.returncode}")

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
