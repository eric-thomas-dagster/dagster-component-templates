"""PagerDutyIncidentSensorComponent.

Polls PagerDuty's REST API for incidents. Triggers a RunRequest per new incident since the cursor (incident `created_at`). Token-authed.
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


class PagerDutyIncidentSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when PagerDuty incidents are created or change state."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    api_key_env_var: str = Field(default="PAGERDUTY_API_KEY", description="Env var with the PagerDuty REST API key.")
    statuses: List[str] = Field(default=["triggered", "acknowledged"], description="Incident statuses to listen for.")
    urgencies: List[str] = Field(default=["high"], description="Incident urgencies to listen for.")


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
            params = [("limit", 100)]
            for s in _self.statuses:
                params.append(("statuses[]", s))
            for u in _self.urgencies:
                params.append(("urgencies[]", u))
            if context.cursor:
                params.append(("since", context.cursor))
            try:
                resp = requests.get(
                    "https://api.pagerduty.com/incidents",
                    headers={"Authorization": f"Token token={tok}", "Accept": "application/vnd.pagerduty+json;version=2"},
                    params=params, timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"PagerDuty error: {e}")
            incidents = resp.json().get("incidents", [])
            if not incidents:
                return SkipReason("no new incidents")
            latest = max(i["created_at"] for i in incidents)
            context.update_cursor(latest)
            return SensorResult(run_requests=[
                RunRequest(run_key=i["id"], asset_selection=targets, tags={"status": i["status"], "urgency": i["urgency"], "service": i.get("service", {}).get("summary", "")[:80]})
                for i in incidents
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
