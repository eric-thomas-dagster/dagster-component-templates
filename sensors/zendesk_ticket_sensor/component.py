"""ZendeskTicketSensorComponent.

Uses Zendesk's Incremental Tickets export API to find tickets created or updated since the cursor. Triggers a RunRequest per new ticket. Token-authed.
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


class ZendeskTicketSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when Zendesk has new or updated tickets matching a query."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    subdomain: str = Field(description="Zendesk subdomain (e.g. 'mycompany' for mycompany.zendesk.com).")
    email: str = Field(description="Zendesk user email used with the API token.")
    api_token_env_var: str = Field(default="ZENDESK_API_TOKEN", description="Env var with the Zendesk API token.")
    statuses: Optional[List[str]] = Field(default=None, description="Optional ticket statuses to filter on (e.g. ['new', 'open']).")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")
            tok = os.environ.get(_self.api_token_env_var, "")
            if not tok:
                return SkipReason(f"{_self.api_token_env_var} not set")
            start_time = int(context.cursor or 0)
            url = f"https://{_self.subdomain}.zendesk.com/api/v2/incremental/tickets.json"
            try:
                resp = requests.get(url, params={"start_time": start_time}, auth=(f"{_self.email}/token", tok), timeout=30)
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"Zendesk error: {e}")
            data = resp.json()
            tickets = data.get("tickets", [])
            if _self.statuses:
                tickets = [t for t in tickets if t.get("status") in _self.statuses]
            if not tickets:
                return SkipReason("no new tickets")
            context.update_cursor(str(data.get("end_time", start_time)))
            return SensorResult(run_requests=[
                RunRequest(run_key=str(t["id"]), asset_selection=targets, tags={"status": t.get("status", ""), "priority": t.get("priority", "") or "none"})
                for t in tickets
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
