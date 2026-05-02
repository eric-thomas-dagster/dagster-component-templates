"""HttpPollSensorComponent.

Generic HTTP polling sensor. Hits a URL on each evaluation, computes a hash
of a *targeted slice* of the response (NOT the whole body — pages have
ad rotations / "last updated" widgets / nonces that change without the
real content changing), and triggers a RunRequest when the hash differs
from the cursor.

Targeting strategies:
- json_path: dotted path into a JSON response (e.g. "data.score" or "0.title")
- regex_extract: hash the concatenation of all regex matches
- css_selector: hash the text content of selected HTML elements (needs bs4)
- strip_patterns: regexes to *remove* from the body before hashing (good for
  pages with known noise like timestamps)
- (none): hash the full response body — fragile, use for stable APIs only

Useful for free public APIs (NBA scores, weather, regulatory filings) or
internal endpoints — no platform-specific SDK required.
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
)
from pydantic import Field


class HttpPollSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Poll an HTTP endpoint and trigger a run when a targeted slice of the response changes."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Asset keys to materialize when the response changes.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")
    url: str = Field(description="URL to poll.")
    method: str = Field(default="GET", description="HTTP method.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="Optional headers.")
    timeout_seconds: int = Field(default=30, description="HTTP timeout.")
    expected_status: int = Field(default=200, description="Expected HTTP status; non-match skips the tick.")

    # --- Targeting (pick one, or combine strip_patterns with anything) ---
    json_path: Optional[str] = Field(
        default=None,
        description="Dotted path into a JSON response (e.g. 'data.score'). Hashes the value at that path.",
    )
    regex_extract: Optional[str] = Field(
        default=None,
        description="Regex pattern; hashes the concatenation of all matches (groups concatenated).",
    )
    css_selector: Optional[str] = Field(
        default=None,
        description="CSS selector (e.g. 'div.scores'). Hashes the text content of all matching elements. Needs bs4.",
    )
    strip_patterns: Optional[List[str]] = Field(
        default=None,
        description="Regexes to REMOVE from the body before hashing. Use to strip known noise (timestamps, ad slots, nonces).",
    )

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

            # Apply targeting strategy in priority order
            value = None
            strategy = "full_body"
            if _self.json_path:
                strategy = "json_path"
                try:
                    obj = resp.json()
                    for part in _self.json_path.split("."):
                        obj = obj[int(part)] if part.isdigit() else obj[part]
                    value = str(obj)
                except Exception as e:
                    return SkipReason(f"json_path '{_self.json_path}' failed: {e}")
            elif _self.regex_extract:
                strategy = "regex_extract"
                import re as _re
                matches = _re.findall(_self.regex_extract, resp.text, flags=_re.DOTALL)
                # Each match may be a tuple (capture groups) or a string
                value = "\n".join(("".join(m) if isinstance(m, tuple) else m) for m in matches)
            elif _self.css_selector:
                strategy = "css_selector"
                try:
                    from bs4 import BeautifulSoup
                except ImportError:
                    return SkipReason("css_selector requires beautifulsoup4 (`pip install beautifulsoup4`)")
                soup = BeautifulSoup(resp.text, "html.parser")
                elements = soup.select(_self.css_selector)
                value = "\n".join(el.get_text(strip=True) for el in elements)
            else:
                value = resp.text

            # Apply strip_patterns (after extraction, before hashing)
            if _self.strip_patterns and value is not None:
                import re as _re
                for pat in _self.strip_patterns:
                    value = _re.sub(pat, "", value, flags=_re.DOTALL)

            if not value:
                return SkipReason(f"strategy '{strategy}' produced no content")

            import hashlib
            digest = hashlib.sha256(value.encode()).hexdigest()
            if context.cursor == digest:
                return SkipReason(f"unchanged (strategy={strategy})")
            context.update_cursor(digest)
            return SensorResult(run_requests=[
                RunRequest(
                    run_key=digest[:12],
                    asset_selection=targets,
                    tags={"hash_strategy": strategy, "digest_prefix": digest[:8]},
                )
            ])

        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
