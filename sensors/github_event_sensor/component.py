"""GithubEventSensorComponent.

Polls the GitHub Events API for a repo and triggers a RunRequest for each new event of the specified types. Cursor stored as last-seen event ID. Personal access token used via env var.
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


class GithubEventSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when a GitHub repo has new events (PRs opened/merged, pushes, issues)."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    repo: str = Field(description="GitHub repo in 'owner/name' form.")
    token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var with a GitHub PAT (or unset for public repos with rate-limited unauthed access).")
    event_types: List[str] = Field(default=["PullRequestEvent", "PushEvent", "IssuesEvent"], description="GitHub event_type names to listen for.")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")
            url = f"https://api.github.com/repos/{_self.repo}/events"
            headers = {"Accept": "application/vnd.github+json"}
            tok = os.environ.get(_self.token_env_var, "")
            if tok:
                headers["Authorization"] = f"Bearer {tok}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"GitHub error: {e}")
            events = resp.json()
            last_seen = context.cursor or ""
            new_events = []
            for e in events:
                if e["id"] == last_seen:
                    break
                if e["type"] in _self.event_types:
                    new_events.append(e)
            if not new_events:
                return SkipReason("no new matching events")
            context.update_cursor(events[0]["id"])
            return SensorResult(run_requests=[
                RunRequest(run_key=e["id"], asset_selection=targets, tags={"event_type": e["type"], "actor": e.get("actor", {}).get("login", "")[:80]})
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
