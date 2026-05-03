"""AssetJobComponent.

Creates a job over the configured asset selection so it shows up as a discrete unit in the Dagster UI's Jobs tab. Useful when you want a one-button 'run this subset' experience without committing to a schedule.
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


class AssetJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Define a named Dagster job from a set of asset keys (without scheduling it)."""

    job_name: str = Field(description="Unique job name shown in the Jobs tab.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys included in the job.")
    description: Optional[str] = Field(default=None, description="Description shown in the UI.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to runs of this job.")

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        job = define_asset_job(
            name=self.job_name,
            selection=AssetSelection.assets(*targets),
            description=self.description,
            tags=self.tags or {},
        )
        return Definitions(jobs=[job])

