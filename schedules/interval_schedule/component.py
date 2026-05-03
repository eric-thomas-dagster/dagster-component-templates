"""IntervalScheduleComponent.

Friendlier than cron when you just want 'every 30 minutes'. Generates a cron expression under the hood, plus the same job + ScheduleDefinition shape as `cron_schedule`.
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from pydantic import Field


class IntervalScheduleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an asset selection at a fixed interval (every N minutes / hours / days)."""

    schedule_name: str = Field(description="Unique schedule name.")
    every: str = Field(description="Interval value + unit, e.g. '15m', '2h', '1d'.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize on each tick.")
    job_name: Optional[str] = Field(default=None, description="Name of the underlying job (defaults to '<schedule_name>_job').")
    execution_timezone: str = Field(default="UTC", description="IANA timezone, e.g. 'America/New_York'.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to runs created by this schedule.")

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        job = define_asset_job(
            name=self.job_name or f"{self.schedule_name}_job",
            selection=AssetSelection.assets(*targets),
        )
        # Convert "every" → cron
        v = self.every.strip().lower()
        if v.endswith("m"):
            n = int(v[:-1])
            cron = f"*/{n} * * * *" if n < 60 else f"0 */{n//60} * * *"
        elif v.endswith("h"):
            n = int(v[:-1])
            cron = f"0 */{n} * * *"
        elif v.endswith("d"):
            n = int(v[:-1])
            cron = f"0 0 */{n} * *"
        else:
            raise ValueError(f"Invalid 'every' value: {self.every}. Use e.g. '15m', '2h', '1d'.")
        sched = ScheduleDefinition(
            name=self.schedule_name,
            cron_schedule=cron,
            job=job,
            execution_timezone=self.execution_timezone,
            default_status=DefaultScheduleStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultScheduleStatus.STOPPED,
            tags=self.tags or {},
        )
        return Definitions(schedules=[sched], jobs=[job])

