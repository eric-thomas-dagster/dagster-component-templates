"""RssFeedSensorComponent.

Polls a public RSS or Atom feed and triggers a RunRequest for each new entry seen since the last evaluation (by entry GUID). Useful for newswire / blog / regulatory-filing-driven pipelines.
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


class RssFeedSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when an RSS / Atom feed publishes a new entry."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    feed_url: str = Field(description="RSS or Atom feed URL.")
    max_entries_per_tick: int = Field(default=20, description="Cap entries emitted per evaluation.")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            try:
                import feedparser
            except ImportError:
                return SkipReason("feedparser not installed")
            feed = feedparser.parse(_self.feed_url)
            if feed.bozo and not feed.entries:
                return SkipReason(f"feed parse error: {feed.bozo_exception}")
            seen = set((context.cursor or "").split(",")) if context.cursor else set()
            new_runs = []
            new_guids = []
            for entry in feed.entries[: _self.max_entries_per_tick]:
                guid = entry.get("id") or entry.get("link") or entry.get("title", "")
                if not guid or guid in seen:
                    continue
                new_guids.append(guid)
                new_runs.append(RunRequest(run_key=guid[:64], asset_selection=targets, tags={"feed_title": entry.get("title", "")[:80]}))
            if not new_runs:
                return SkipReason("no new entries")
            # Keep a bounded cursor (last 200 GUIDs)
            cursor_set = list(seen) + new_guids
            context.update_cursor(",".join(cursor_set[-200:]))
            return SensorResult(run_requests=new_runs)


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
