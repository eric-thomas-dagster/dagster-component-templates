"""Salesforce Workspace Component.

StateBackedComponent that auto-enumerates SObjects (standard + custom) from
a Salesforce org via `/services/data/vXX.0/sobjects`, then emits one Dagster
asset per SObject. Materializing an asset fetches recent records via SOQL.

Pairs with the existing `salesforce_resource` for auth. Peer of the low-level
`salesforce_ingestion` (single-object) — this is the workspace-shape sibling.
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
class SObjectSelector(dg.Resolvable):
    """Selector for filtering Salesforce SObjects. Fivetran-shape."""
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None
    include_custom_only: bool = False  # skip standard SObjects if true

    def matches(self, name: str, is_custom: bool = False) -> bool:
        import fnmatch
        if self.include_custom_only and not is_custom:
            return False
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


def _enumerate_sobjects(instance_url, access_token, api_version, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    out: dict = {"sobjects": []}
    try:
        r = session.get(
            f"{instance_url.rstrip('/')}/services/data/v{api_version}/sobjects",
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        body = r.json() or {}
        for s in body.get("sobjects", []):
            name = s.get("name")
            if name:
                out["sobjects"].append({
                    "name": name,
                    "label": s.get("label", name),
                    "custom": bool(s.get("custom", False)),
                    "queryable": bool(s.get("queryable", True)),
                })
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class SalesforceWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per Salesforce SObject."""

        instance_url_env_var: str
        access_token_env_var: str
        api_version: str = "58.0"
        verify_ssl: bool = True

        sobject_selector: Optional[SObjectSelector] = None

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["salesforce"])
        compute_kind: str = "salesforce"

        query_limit: int = 1000
        query_fields: Optional[List[str]] = None  # None = "SELECT FIELDS(STANDARD)"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"SalesforceWorkspace[{hashlib.sha256(self.instance_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            iu = os.environ.get(self.instance_url_env_var, "")
            tok = os.environ.get(self.access_token_env_var, "")
            snapshot = _enumerate_sobjects(iu, tok, self.api_version, self.verify_ssl)
            if self.sobject_selector is not None:
                snapshot["sobjects"] = [
                    s for s in snapshot["sobjects"]
                    if self.sobject_selector.matches(s["name"], s.get("custom", False))
                    and s.get("queryable", True)
                ]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for s in state.get("sobjects", []):
                assets.append(self._build_asset(s))
            return dg.Definitions(assets=assets)

        def _build_asset(self, sobj: dict):
            _self = self
            name = sobj["name"]
            key = dg.AssetKey([*self.asset_key_prefix, name])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "sobject_name": dg.MetadataValue.text(name),
                    "sobject_label": dg.MetadataValue.text(sobj.get("label", name)),
                    "custom": dg.MetadataValue.bool(sobj.get("custom", False)),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                iu = os.environ.get(_self.instance_url_env_var, "")
                tok = os.environ.get(_self.access_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}

                if _self.query_fields:
                    fields = ", ".join(_self.query_fields)
                else:
                    fields = "FIELDS(STANDARD)"
                query = f"SELECT {fields} FROM {name} LIMIT {_self.query_limit}"

                r = session.get(
                    f"{iu.rstrip('/')}/services/data/v{_self.api_version}/query",
                    headers=headers,
                    params={"q": query},
                    timeout=120,
                )
                r.raise_for_status()
                body = r.json() or {}
                rows = body.get("records", [])
                # Strip 'attributes' — Salesforce adds this to every record.
                for row in rows:
                    row.pop("attributes", None)
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "sobject": name,
                    "query": query[:200],
                })
                return df

            return _asset

else:
    class SalesforceWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "SalesforceWorkspaceComponent requires Dagster with StateBackedComponent support."
            )
