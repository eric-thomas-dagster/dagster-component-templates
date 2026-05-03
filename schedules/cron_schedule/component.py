"""CronScheduleComponent.

Defines a `ScheduleDefinition` + an underlying asset job that materializes the configured asset keys whenever the cron expression fires. Replaces the ad-hoc `dg launch --assets '*'` pattern with a real Dagster schedule that shows up in the UI.
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


class CronScheduleComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an asset selection on a cron schedule."""

    schedule_name: str = Field(description="Unique schedule name shown in the UI.")
    cron_expression: str = Field(description="Cron expression (5 fields: m h dom mon dow). E.g. '0 9 * * *' = daily 09:00.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize on each tick.")
    job_name: Optional[str] = Field(default=None, description="Name of the underlying job (defaults to '<schedule_name>_job').")
    execution_timezone: str = Field(default="UTC", description="IANA timezone for cron evaluation, e.g. 'America/Los_Angeles'.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' (live) or 'STOPPED' (dormant; user must enable).")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to runs created by this schedule.")

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        job = define_asset_job(
            name=self.job_name or f"{self.schedule_name}_job",
            selection=AssetSelection.assets(*targets),
        )
        sched = ScheduleDefinition(
            name=self.schedule_name,
            cron_schedule=self.cron_expression,
            job=job,
            execution_timezone=self.execution_timezone,
            default_status=DefaultScheduleStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultScheduleStatus.STOPPED,
            tags=self.tags or {},
        )
        return Definitions(schedules=[sched], jobs=[job])

