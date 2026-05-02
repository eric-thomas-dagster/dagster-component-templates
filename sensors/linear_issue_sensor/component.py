"""LinearIssueSensorComponent.

Polls Linear's GraphQL API for issues updated since the cursor. Triggers a RunRequest per matching issue. Token-authed.
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


class LinearIssueSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when Linear has new or updated issues matching a team/state filter."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    api_key_env_var: str = Field(default="LINEAR_API_KEY", description="Env var with the Linear API key.")
    team_id: Optional[str] = Field(default=None, description="Optional Linear team UUID; omit for all teams.")
    state_types: List[str] = Field(default=["unstarted", "started"], description="Workflow state types to listen for.")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")
            tok = os.environ.get(_self.api_key_env_var, "")
            if not tok:
                return SkipReason(f"{_self.api_key_env_var} not set")
            since = context.cursor or "1970-01-01T00:00:00Z"
            filt = f'updatedAt: {{ gt: "{since}" }}, state: {{ type: {{ in: [{", ".join(repr(s) for s in _self.state_types)}] }} }}'
            if _self.team_id:
                filt = f'team: {{ id: {{ eq: "{_self.team_id}" }} }}, ' + filt
            query = f'{{ issues(filter: {{ {filt} }}, first: 50, orderBy: updatedAt) {{ nodes {{ id identifier title updatedAt state {{ type name }} }} }} }}'
            try:
                resp = requests.post(
                    "https://api.linear.app/graphql",
                    json={"query": query},
                    headers={"Authorization": tok, "Content-Type": "application/json"},
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"Linear error: {e}")
            nodes = resp.json().get("data", {}).get("issues", {}).get("nodes", [])
            if not nodes:
                return SkipReason("no new issues")
            context.update_cursor(max(n["updatedAt"] for n in nodes))
            return SensorResult(run_requests=[
                RunRequest(run_key=n["id"], asset_selection=targets, tags={"identifier": n["identifier"], "state": n["state"]["name"]})
                for n in nodes
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
