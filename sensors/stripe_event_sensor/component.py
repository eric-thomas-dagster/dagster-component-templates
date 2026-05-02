"""StripeEventSensorComponent.

Polls Stripe's Events API and triggers a RunRequest for each new event of the specified types. Cursor stored as last-seen event ID (Stripe events are ordered).
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


class StripeEventSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when Stripe emits new events (charges, subscriptions, invoices, etc)."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    api_key_env_var: str = Field(default="STRIPE_API_KEY", description="Env var with the Stripe secret key.")
    event_types: List[str] = Field(default=["charge.succeeded", "invoice.payment_succeeded", "customer.subscription.created"], description="Stripe event types to listen for.")
    limit_per_tick: int = Field(default=100, description="Max events per evaluation.")


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
            params = [("limit", _self.limit_per_tick)]
            for et in _self.event_types:
                params.append(("types[]", et))
            if context.cursor:
                params.append(("starting_after", context.cursor))
            try:
                resp = requests.get("https://api.stripe.com/v1/events", auth=(tok, ""), params=params, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"Stripe error: {e}")
            data = resp.json().get("data", [])
            if not data:
                return SkipReason("no new events")
            data.reverse()  # oldest first
            context.update_cursor(data[-1]["id"])
            return SensorResult(run_requests=[
                RunRequest(run_key=ev["id"], asset_selection=targets, tags={"event_type": ev["type"]})
                for ev in data
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
