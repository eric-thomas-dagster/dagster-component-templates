"""HubSpot Workspace Component.

StateBackedComponent that auto-enumerates HubSpot CRM objects (contacts,
companies, deals, tickets, plus custom objects) and emits one Dagster asset
per object type. Materializing an asset fetches recent records via the CRM
Objects API.
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

STANDARD_OBJECTS = ["contacts", "companies", "deals", "tickets", "products", "line_items"]


@dataclass
class HubSpotObjectSelector(dg.Resolvable):
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


def _enumerate_hubspot(access_token, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    out: dict = {"objects": []}
    # Standard objects.
    for obj in STANDARD_OBJECTS:
        out["objects"].append({"name": obj, "custom": False})

    # Custom object schemas.
    try:
        r = session.get("https://api.hubapi.com/crm/v3/schemas", headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json() or {}
        for s in body.get("results", []):
            name = s.get("fullyQualifiedName") or s.get("objectTypeId") or s.get("name")
            if name and name not in STANDARD_OBJECTS:
                out["objects"].append({"name": name, "custom": True})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class HubSpotWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        access_token_env_var: str
        verify_ssl: bool = True
        object_selector: Optional[HubSpotObjectSelector] = None
        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["hubspot"])
        compute_kind: str = "hubspot"
        page_limit: int = 100
        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"HubSpotWorkspace[{hashlib.sha256(self.access_token_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            tok = os.environ.get(self.access_token_env_var, "")
            snapshot = _enumerate_hubspot(tok, self.verify_ssl)
            if self.object_selector is not None:
                snapshot["objects"] = [o for o in snapshot["objects"] if self.object_selector.matches(o["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for o in state.get("objects", []):
                assets.append(self._build_asset(o["name"], o.get("custom", False)))
            return dg.Definitions(assets=assets)

        def _build_asset(self, obj_name: str, is_custom: bool):
            _self = self
            safe = obj_name.replace(".", "_").replace("-", "_")
            key = dg.AssetKey([*self.asset_key_prefix, safe])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "hubspot_object": dg.MetadataValue.text(obj_name),
                    "custom": dg.MetadataValue.bool(is_custom),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                tok = os.environ.get(_self.access_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}

                url = f"https://api.hubapi.com/crm/v3/objects/{obj_name}"
                r = session.get(url, headers=headers, params={"limit": _self.page_limit}, timeout=60)
                r.raise_for_status()
                body = r.json() or {}
                results = body.get("results", [])
                # Flatten each record's properties dict up to top level.
                flat_rows = []
                for record in results:
                    row = {"id": record.get("id")}
                    row.update(record.get("properties") or {})
                    flat_rows.append(row)
                df = pd.DataFrame(flat_rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "hubspot_object": obj_name,
                })
                return df

            return _asset

else:
    class HubSpotWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("HubSpotWorkspaceComponent requires Dagster with StateBackedComponent support.")
