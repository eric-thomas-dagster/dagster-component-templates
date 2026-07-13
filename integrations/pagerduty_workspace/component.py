"""PagerDuty Workspace Component.

Auto-enumerates PagerDuty services and emits one Dagster asset per service.
Materializing an asset fetches recent incidents for the service and emits
a DataFrame.
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

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
class PagerDutyServiceSelector(dg.Resolvable):
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


def _enumerate_pd(api_token, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {
        "Authorization": f"Token token={api_token}",
        "Accept": "application/vnd.pagerduty+json;version=2",
    }

    out: dict = {"services": []}
    offset = 0
    try:
        while True:
            r = session.get(
                "https://api.pagerduty.com/services",
                headers=headers, params={"limit": 100, "offset": offset},
                timeout=60,
            )
            r.raise_for_status()
            body = r.json() or {}
            services = body.get("services", [])
            for svc in services:
                sid = svc.get("id")
                sname = svc.get("name")
                if sid and sname:
                    out["services"].append({"id": sid, "name": sname, "status": svc.get("status")})
            if not body.get("more"):
                break
            offset += len(services) if services else 100
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class PagerDutyWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        api_token_env_var: str
        verify_ssl: bool = True

        service_selector: Optional[PagerDutyServiceSelector] = None
        incidents_since_days: int = 30
        incidents_limit: int = 100
        statuses: List[str] = field(default_factory=lambda: ["triggered", "acknowledged", "resolved"])

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["pagerduty"])
        compute_kind: str = "pagerduty"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"PagerDutyWorkspace[{hashlib.sha256(self.api_token_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            tok = os.environ.get(self.api_token_env_var, "")
            snapshot = _enumerate_pd(tok, self.verify_ssl)
            if self.service_selector is not None:
                snapshot["services"] = [s for s in snapshot["services"] if self.service_selector.matches(s["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for s in state.get("services", []):
                assets.append(self._build_asset(s["id"], s["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, service_id: str, service_name: str):
            _self = self
            safe = "".join(c if c.isalnum() or c == "_" else "_" for c in service_name)[:60] or service_id
            key = dg.AssetKey([*self.asset_key_prefix, safe])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "pd_service_id": dg.MetadataValue.text(service_id),
                    "pd_service_name": dg.MetadataValue.text(service_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                import time
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                tok = os.environ.get(_self.api_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {
                    "Authorization": f"Token token={tok}",
                    "Accept": "application/vnd.pagerduty+json;version=2",
                }

                since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - _self.incidents_since_days * 86400))
                params: dict = {
                    "service_ids[]": service_id,
                    "since": since,
                    "limit": _self.incidents_limit,
                    "statuses[]": _self.statuses,
                }
                r = session.get("https://api.pagerduty.com/incidents", headers=headers, params=params, timeout=60)
                r.raise_for_status()
                incidents = (r.json() or {}).get("incidents", [])
                rows = [{
                    "id": inc.get("id"),
                    "incident_number": inc.get("incident_number"),
                    "title": inc.get("title"),
                    "status": inc.get("status"),
                    "urgency": inc.get("urgency"),
                    "created_at": inc.get("created_at"),
                    "resolved_at": inc.get("resolved_at"),
                    "assignee": ((inc.get("assignments") or [{}])[0].get("assignee") or {}).get("summary"),
                } for inc in incidents]
                df = pd.DataFrame(rows)
                context.add_output_metadata({"row_count": len(df), "pd_service": service_name})
                return df

            return _asset

else:
    class PagerDutyWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("PagerDutyWorkspaceComponent requires Dagster with StateBackedComponent support.")
