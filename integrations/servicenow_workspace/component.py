"""ServiceNow Workspace Component.

Auto-enumerates ServiceNow tables from sys_db_object and emits one Dagster
asset per table. Materializing an asset reads recent records via the Table API.
"""
import base64
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
class ServiceNowTableSelector(dg.Resolvable):
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


def _enumerate_tables(instance, username, password, verify_ssl, limit=1000) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    session.auth = (username, password)
    headers = {"Accept": "application/json"}

    out: dict = {"tables": []}
    try:
        url = f"https://{instance}.service-now.com/api/now/table/sys_db_object"
        r = session.get(url, headers=headers, params={
            "sysparm_fields": "name,label",
            "sysparm_limit": limit,
        }, timeout=60)
        r.raise_for_status()
        body = r.json() or {}
        for t in body.get("result", []):
            name = t.get("name")
            label = t.get("label") or name
            if name:
                out["tables"].append({"name": name, "label": label})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class ServiceNowWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        instance_env_var: str
        username_env_var: str
        password_env_var: str
        verify_ssl: bool = True

        table_selector: Optional[ServiceNowTableSelector] = None
        query: Optional[str] = None            # sysparm_query filter
        query_limit: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["servicenow"])
        compute_kind: str = "servicenow"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"ServiceNowWorkspace[{hashlib.sha256(self.instance_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            inst = os.environ.get(self.instance_env_var, "")
            u = os.environ.get(self.username_env_var, "")
            p = os.environ.get(self.password_env_var, "")
            snapshot = _enumerate_tables(inst, u, p, self.verify_ssl)
            if self.table_selector is not None:
                snapshot["tables"] = [t for t in snapshot["tables"] if self.table_selector.matches(t["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for t in state.get("tables", []):
                assets.append(self._build_asset(t["name"], t.get("label", "")))
            return dg.Definitions(assets=assets)

        def _build_asset(self, tname: str, label: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, tname])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "servicenow_table": dg.MetadataValue.text(tname),
                    "table_label": dg.MetadataValue.text(label),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                inst = os.environ.get(_self.instance_env_var, "")
                u = os.environ.get(_self.username_env_var, "")
                p = os.environ.get(_self.password_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                session.auth = (u, p)
                headers = {"Accept": "application/json"}

                params: dict = {"sysparm_limit": _self.query_limit}
                if _self.query:
                    params["sysparm_query"] = _self.query
                r = session.get(
                    f"https://{inst}.service-now.com/api/now/table/{tname}",
                    headers=headers, params=params, timeout=120,
                )
                r.raise_for_status()
                body = r.json() or {}
                results = body.get("result", [])
                df = pd.DataFrame(results)
                context.add_output_metadata({
                    "row_count": len(df),
                    "servicenow_table": tname,
                })
                return df

            return _asset

else:
    class ServiceNowWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("ServiceNowWorkspaceComponent requires Dagster with StateBackedComponent support.")
