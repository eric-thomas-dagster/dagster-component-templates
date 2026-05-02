"""JiraIssueSensorComponent.

Polls Jira's search API with a JQL query (auto-augmented with `updated >= cursor`). Triggers a RunRequest per new/updated issue. Basic-authed via API token.
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


class JiraIssueSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when Jira has new or updated issues matching a JQL query."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    base_url: str = Field(description="Jira base URL (e.g. https://mycompany.atlassian.net).")
    email: str = Field(description="Atlassian email.")
    api_token_env_var: str = Field(default="JIRA_API_TOKEN", description="Env var with the Atlassian API token.")
    jql: str = Field(description="Base JQL query (e.g. 'project = OPS AND status != Done').")
    fields: List[str] = Field(default=["summary", "status", "priority", "assignee"], description="Fields to fetch per issue.")


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
            jql = _self.jql
            if context.cursor:
                jql = f"{jql} AND updated > \"{context.cursor}\""
            jql = f"{jql} ORDER BY updated ASC"
            try:
                resp = requests.get(
                    f"{_self.base_url}/rest/api/3/search",
                    params={"jql": jql, "fields": ",".join(_self.fields), "maxResults": 100},
                    auth=(_self.email, tok),
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                return SkipReason(f"Jira error: {e}")
            issues = resp.json().get("issues", [])
            if not issues:
                return SkipReason("no new issues")
            latest_updated = max(i["fields"].get("updated", "") for i in issues if i["fields"].get("updated"))
            if latest_updated:
                # Strip TZ for JQL compatibility (e.g. drop ".000+0000")
                context.update_cursor(latest_updated[:16].replace("T", " "))
            return SensorResult(run_requests=[
                RunRequest(run_key=i["key"], asset_selection=targets, tags={"status": i["fields"].get("status", {}).get("name", ""), "priority": i["fields"].get("priority", {}).get("name", "") if i["fields"].get("priority") else ""})
                for i in issues
            ])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
