"""NotionDatabaseSensorComponent.

Polls a Notion database via the official API. Triggers a RunRequest for each row whose `last_edited_time` is after the cursor. Token-authed.
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


class NotionDatabaseSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when a Notion database has new or updated rows."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    database_id: str = Field(description="Notion database ID (UUID).")
    api_key_env_var: str = Field(default="NOTION_API_KEY", description="Env var with the Notion integration token.")
    page_size: int = Field(default=50, description="Rows fetched per query.")


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
            try:
                resp = requests.post(
                    f"https://api.notion.com/v1/databases/{_self.database_id}/query",
                    headers={"Authorization": f"Bearer {tok}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                    json={
                        "page_size": _self.page_size,
                        "filter": {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": since}},
                        "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
                    },
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"Notion error: {e}")
            results = resp.json().get("results", [])
            if not results:
                return SkipReason("no new rows")
            latest = max(r["last_edited_time"] for r in results)
            context.update_cursor(latest)
            return SensorResult(run_requests=[
                RunRequest(run_key=r["id"], asset_selection=targets, tags={"last_edited_time": r["last_edited_time"][:19]})
                for r in results
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
