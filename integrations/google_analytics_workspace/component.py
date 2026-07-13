"""Google Analytics Workspace Component.

Auto-enumerates GA4 Admin properties × data streams and emits one Dagster
asset per (property, dataStream). Materializing an asset runs a
runReport against the GA Data API for a default metric/dimension set,
returning a DataFrame.
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


@dataclass
class GAPropertySelector(dg.Resolvable):
    """Selector for filtering GA properties. Fivetran-shape."""
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _get_access_token(credentials_json: str) -> str:
    """Exchange a service-account credentials JSON for an OAuth token."""
    import json as _json
    import time
    import base64
    import requests

    try:
        import jwt as _jwt  # pyjwt
    except ImportError as e:
        raise Exception("pyjwt required — pip install pyjwt cryptography") from e

    creds = _json.loads(credentials_json)
    now = int(time.time())
    claims = {
        "iss": creds["client_email"],
        "scope": "https://www.googleapis.com/auth/analytics.readonly",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }
    signed = _jwt.encode(claims, creds["private_key"], algorithm="RS256")
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": signed},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _enumerate_ga(credentials_json, verify_ssl) -> dict:
    import requests
    try:
        token = _get_access_token(credentials_json)
    except Exception:
        return {"accounts": []}

    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {token}"}

    out: dict = {"accounts": []}
    try:
        r = session.get("https://analyticsadmin.googleapis.com/v1beta/accounts", headers=headers, timeout=30)
        r.raise_for_status()
        for acct in (r.json() or {}).get("accounts", []):
            acct_name = acct.get("name", "")  # e.g. "accounts/1234"
            display = acct.get("displayName", acct_name)
            properties: list = []
            try:
                pr = session.get(
                    "https://analyticsadmin.googleapis.com/v1beta/properties",
                    headers=headers,
                    params={"filter": f"parent:{acct_name}"},
                    timeout=30,
                )
                pr.raise_for_status()
                for prop in (pr.json() or {}).get("properties", []):
                    prop_name = prop.get("name", "")  # e.g. "properties/12345"
                    prop_display = prop.get("displayName", prop_name)
                    properties.append({"name": prop_name, "display_name": prop_display})
            except Exception:  # noqa: BLE001
                pass
            out["accounts"].append({"name": acct_name, "display_name": display, "properties": properties})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class GoogleAnalyticsWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        credentials_json_env_var: str  # inline service-account creds JSON
        verify_ssl: bool = True

        property_selector: Optional[GAPropertySelector] = None
        metrics: List[str] = field(default_factory=lambda: ["activeUsers", "sessions", "eventCount"])
        dimensions: List[str] = field(default_factory=lambda: ["date"])
        date_range_days: int = 30

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["ga"])
        compute_kind: str = "google_analytics"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"GoogleAnalyticsWorkspace[{hashlib.sha256(self.credentials_json_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            creds = os.environ.get(self.credentials_json_env_var, "")
            snapshot = _enumerate_ga(creds, self.verify_ssl)
            if self.property_selector is not None:
                for a in snapshot["accounts"]:
                    a["properties"] = [p for p in a["properties"] if self.property_selector.matches(p["display_name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for a in state.get("accounts", []):
                for p in a.get("properties", []):
                    assets.append(self._build_asset(a["display_name"], p["name"], p["display_name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, account_display: str, property_resource: str, property_display: str):
            _self = self
            # property_resource looks like "properties/12345"
            prop_id = property_resource.split("/", 1)[-1]
            safe_acct = "".join(c if c.isalnum() or c == "_" else "_" for c in account_display)[:40] or "acct"
            safe_prop = "".join(c if c.isalnum() or c == "_" else "_" for c in property_display)[:40] or f"p_{prop_id}"
            key = dg.AssetKey([*self.asset_key_prefix, safe_acct, safe_prop])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "ga_property_id": dg.MetadataValue.text(prop_id),
                    "ga_property_display": dg.MetadataValue.text(property_display),
                    "ga_account": dg.MetadataValue.text(account_display),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                creds = os.environ.get(_self.credentials_json_env_var, "")
                token = _get_access_token(creds)
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

                body: Dict[str, Any] = {
                    "dateRanges": [{"startDate": f"{_self.date_range_days}daysAgo", "endDate": "today"}],
                    "metrics": [{"name": m} for m in _self.metrics],
                    "dimensions": [{"name": d} for d in _self.dimensions],
                }
                url = f"https://analyticsdata.googleapis.com/v1beta/properties/{prop_id}:runReport"
                r = session.post(url, json=body, headers=headers, timeout=120)
                r.raise_for_status()
                payload = r.json() or {}

                dim_names = [d.get("name") for d in payload.get("dimensionHeaders", [])]
                met_names = [m.get("name") for m in payload.get("metricHeaders", [])]
                rows = []
                for row in payload.get("rows", []):
                    r_out = {}
                    for i, dv in enumerate(row.get("dimensionValues", [])):
                        if i < len(dim_names):
                            r_out[dim_names[i]] = dv.get("value")
                    for i, mv in enumerate(row.get("metricValues", [])):
                        if i < len(met_names):
                            r_out[met_names[i]] = mv.get("value")
                    rows.append(r_out)
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "ga_property": property_display,
                })
                return df

            return _asset

else:
    class GoogleAnalyticsWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("GoogleAnalyticsWorkspaceComponent requires Dagster with StateBackedComponent support.")
