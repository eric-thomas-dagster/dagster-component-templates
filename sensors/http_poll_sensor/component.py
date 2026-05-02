"""HttpPollSensorComponent.

Generic HTTP polling sensor. Hits a URL on each evaluation, hashes the response body (or extracts a JSONPath value), and triggers a RunRequest when the hash/value changes vs the cursor. Useful for free public APIs or webhooks landing on a known URL — no platform-specific SDK required.
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


class HttpPollSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Poll an HTTP endpoint and trigger a run when the response changes (by hash or JSONPath value)."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when an event matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    url: str = Field(description="URL to poll.")
    method: str = Field(default="GET", description="HTTP method (GET / POST).")
    headers: Optional[Dict[str, str]] = Field(default=None, description="Optional headers (use env-var values for secrets).")
    json_path: Optional[str] = Field(default=None, description="Optional JSONPath-ish dotted path (e.g. 'data.count'). If unset, hash the full response body.")
    timeout_seconds: int = Field(default=30, description="HTTP timeout.")
    expected_status: int = Field(default=200, description="Expected HTTP status; non-match → skip.")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]

        def sensor_fn(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")
            try:
                resp = requests.request(_self.method, _self.url, headers=_self.headers or None, timeout=_self.timeout_seconds)
            except Exception as e:
                return SkipReason(f"HTTP error: {e}")
            if resp.status_code != _self.expected_status:
                return SkipReason(f"unexpected status {resp.status_code}")
            value = resp.text
            if _self.json_path:
                try:
                    obj = resp.json()
                    for part in _self.json_path.split("."):
                        obj = obj[part] if not part.isdigit() else obj[int(part)]
                    value = str(obj)
                except Exception as e:
                    return SkipReason(f"JSONPath '{_self.json_path}' failed: {e}")
            import hashlib
            digest = hashlib.sha256(value.encode()).hexdigest()
            if context.cursor == digest:
                return SkipReason("response unchanged")
            context.update_cursor(digest)
            return SensorResult(run_requests=[RunRequest(run_key=digest[:12], asset_selection=targets)])


        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
