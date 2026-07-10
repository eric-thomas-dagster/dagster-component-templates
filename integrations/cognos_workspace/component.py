"""Cognos Workspace Component.

StateBackedComponent that auto-enumerates every Cognos report and emits
one Dagster asset per report. Materializing an asset runs the report via
REST.
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
class CognosReportSelector(dg.Resolvable):
    """Selector for filtering Cognos reports. Same shape as Fivetran's connector_selector."""
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


def _enumerate_cognos(base_url, username, password, namespace, verify_ssl, folder_ids) -> dict:
    """Return {reports: [{id, name, folder}]}."""
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/api/v1"

    # Login.
    login_body = {
        "parameters": [
            {"name": "CAMNamespace", "value": namespace or ""},
            {"name": "CAMUsername", "value": username or ""},
            {"name": "CAMPassword", "value": password or ""},
        ]
    }
    try:
        r = session.post(f"{api_base}/session", json=login_body, timeout=30)
        r.raise_for_status()
    except Exception:
        return {"reports": []}

    reports: list = []
    folders_to_walk = folder_ids or ["/"]

    for folder in folders_to_walk:
        try:
            r = session.get(f"{api_base}/content{folder}/items?type=report", timeout=30)
            r.raise_for_status()
            body = r.json() or {}
            items = body.get("data") or body.get("items") or body.get("value") or []
            for it in items:
                rid = it.get("id") or it.get("storeID") or it.get("searchPath")
                rname = it.get("defaultName") or it.get("name") or ""
                if rid and rname:
                    reports.append({"id": rid, "name": rname, "folder": folder})
        except Exception:  # noqa: BLE001
            continue

    return {"reports": reports}


if _HAS_STATE_BACKED:

    @dataclass
    class CognosWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per Cognos report."""

        base_url_env_var: str
        username_env_var: str
        password_env_var: str
        namespace_env_var: str
        verify_ssl: bool = True

        folder_ids: Optional[List[str]] = None  # None = / (root)
        report_selector: Optional[CognosReportSelector] = None

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["cognos", "report"])
        compute_kind: str = "cognos"

        output_format: str = "CSV"
        wait_for_completion: bool = True
        timeout_seconds: int = 600

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"CognosWorkspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            username = os.environ.get(self.username_env_var, "")
            password = os.environ.get(self.password_env_var, "")
            namespace = os.environ.get(self.namespace_env_var, "")

            snapshot = _enumerate_cognos(base_url, username, password, namespace, self.verify_ssl, self.folder_ids)
            if self.report_selector is not None:
                snapshot["reports"] = [r for r in snapshot["reports"] if self.report_selector.matches(r["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for r in state.get("reports", []):
                assets.append(self._build_asset(r["id"], r["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, report_id: str, name: str):
            _self = self
            # Sanitize name → valid asset-key segment.
            safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
            key = dg.AssetKey([*self.asset_key_prefix, safe_name])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "cognos_report_id": dg.MetadataValue.text(report_id),
                    "cognos_report_name": dg.MetadataValue.text(name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import requests
                except ImportError as e:
                    raise Exception("requests library not installed") from e

                base_url = os.environ.get(_self.base_url_env_var, "")
                username = os.environ.get(_self.username_env_var, "")
                password = os.environ.get(_self.password_env_var, "")
                namespace = os.environ.get(_self.namespace_env_var, "")

                session = requests.Session()
                session.verify = _self.verify_ssl
                api_base = f"{base_url.rstrip('/')}/api/v1"

                r = session.post(f"{api_base}/session", json={"parameters": [
                    {"name": "CAMNamespace", "value": namespace},
                    {"name": "CAMUsername", "value": username},
                    {"name": "CAMPassword", "value": password},
                ]}, timeout=30)
                if r.status_code >= 300:
                    raise Exception(f"Cognos login failed: {r.status_code}")

                run_url = f"{api_base}/reports/{report_id}/data"
                body = {"format": _self.output_format}
                rr = session.post(run_url, json=body, timeout=_self.timeout_seconds)
                if rr.status_code >= 400:
                    raise Exception(f"Cognos report run failed: {rr.status_code} {rr.text[:200]}")
                context.log.info(f"Cognos: report {report_id} ({name}) executed (status={rr.status_code})")

            return _asset

else:
    class CognosWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "CognosWorkspaceComponent requires Dagster with StateBackedComponent support "
                "(post-2026 dagster>=1.11 or later)."
            )
