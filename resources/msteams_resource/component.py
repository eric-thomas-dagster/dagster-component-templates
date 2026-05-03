"""MSTeamsResourceComponent.

Wraps `dagster-msteams`'s `MSTeamsResource` so any asset / op can post messages to a Teams channel via an incoming webhook. Pair with hooks (failure_hook, etc.) for run-status notifications.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class MSTeamsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Microsoft Teams notification resource via dagster-msteams."""

    webhook_url_env_var: str = Field(default="MSTEAMS_WEBHOOK_URL", description="Env var with the incoming-webhook URL.")
    timeout_seconds: int = Field(default=60, description="HTTP timeout.")
    resource_key: str = Field(default="msteams", description="Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_msteams import MSTeamsResource
        resource = MSTeamsResource(
            hook_url=dg.EnvVar(self.webhook_url_env_var),
            timeout=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})

