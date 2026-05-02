"""GitlabEventSensorComponent.

Polls the GitLab Events API for a project. Triggers a RunRequest for each new event since the cursor. Token-authed (project access token or PAT).
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from pydantic import Field


class GitlabEventSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when a GitLab project has new events (push, merge, issue, comment)."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    project_id: str = Field(description="GitLab project ID or URL-encoded path (e.g. 'my-org%2Fmy-repo').")
    token_env_var: str = Field(default="GITLAB_TOKEN", description="Env var with a GitLab PAT or project token.")
    base_url: str = Field(default="https://gitlab.com", description="GitLab instance base URL (override for self-hosted).")
    actions: List[str] = Field(default=["pushed", "merged", "opened", "closed"], description="Event 'action_name' values to filter on.")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")
            tok = os.environ.get(_self.token_env_var, "")
            if not tok:
                return SkipReason(f"{_self.token_env_var} not set")
            url = f"{_self.base_url}/api/v4/projects/{_self.project_id}/events"
            try:
                resp = requests.get(url, headers={"PRIVATE-TOKEN": tok}, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"GitLab error: {e}")
            events = resp.json()
            last_seen = int(context.cursor or "0")
            new_events = [e for e in events if e["id"] > last_seen and e.get("action_name") in _self.actions]
            if not new_events:
                return SkipReason("no new matching events")
            context.update_cursor(str(max(e["id"] for e in events)))
            return SensorResult(run_requests=[
                RunRequest(run_key=str(e["id"]), asset_selection=targets, tags={"action_name": e.get("action_name", ""), "author": e.get("author_username", "")[:80]})
                for e in new_events
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
