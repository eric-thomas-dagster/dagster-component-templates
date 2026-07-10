"""Qlik Compose Workspace Component.

StateBackedComponent that auto-enumerates every Qlik Compose project
(data warehouse) × workflow × data mart via the Compose REST API and
emits one Dagster asset per object.
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
class ComposeObjectSelector(dg.Resolvable):
    """Selector for filtering Compose objects (workflows / data marts).

    Same shape as Fivetran's connector_selector.
    """
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


def _enumerate_compose(base_url, username, password, api_token, verify_ssl, projects_filter) -> dict:
    """Return {projects: [{name, workflows: [...], data_marts: [...]}]}."""
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/qlikcompose/api/v1"

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    else:
        r = session.post(f"{api_base}/login", json={"username": username, "password": password}, timeout=30)
        r.raise_for_status()

    out: dict = {"projects": []}
    try:
        r = session.get(f"{api_base}/projects", headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json() or {}
        proj_list = body.get("projects") or body.get("value") or []
    except Exception:
        return out

    for p in proj_list:
        pname = p.get("name") if isinstance(p, dict) else str(p)
        if not pname:
            continue
        if projects_filter is not None and pname not in projects_filter:
            continue

        wfs, marts = [], []
        try:
            wr = session.get(f"{api_base}/projects/{pname}/workflows", headers=headers, timeout=30)
            wr.raise_for_status()
            wbody = wr.json() or {}
            wf_items = wbody.get("workflows") or wbody.get("value") or wbody
            if isinstance(wf_items, list):
                for w in wf_items:
                    wname = w.get("name") if isinstance(w, dict) else str(w)
                    if wname:
                        wfs.append({"name": wname})
        except Exception:  # noqa: BLE001
            pass

        try:
            dr = session.get(f"{api_base}/projects/{pname}/data_marts", headers=headers, timeout=30)
            dr.raise_for_status()
            dbody = dr.json() or {}
            mart_items = dbody.get("data_marts") or dbody.get("value") or dbody
            if isinstance(mart_items, list):
                for m in mart_items:
                    mname = m.get("name") if isinstance(m, dict) else str(m)
                    if mname:
                        marts.append({"name": mname})
        except Exception:  # noqa: BLE001
            pass

        out["projects"].append({"name": pname, "workflows": wfs, "data_marts": marts})
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class QlikComposeWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per Compose Workflow / Data Mart."""

        base_url_env_var: str
        username_env_var: Optional[str] = None
        password_env_var: Optional[str] = None
        api_token_env_var: Optional[str] = None
        verify_ssl: bool = True

        projects: Optional[List[str]] = None  # None = all projects
        workflow_selector: Optional[ComposeObjectSelector] = None
        data_mart_selector: Optional[ComposeObjectSelector] = None

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["qlik_compose"])
        compute_kind: str = "qlik_compose"

        wait_for_completion: bool = True
        poll_interval_seconds: int = 30
        timeout_seconds: int = 3600

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"QlikComposeWorkspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            username = os.environ.get(self.username_env_var, "") if self.username_env_var else None
            password = os.environ.get(self.password_env_var, "") if self.password_env_var else None
            api_token = os.environ.get(self.api_token_env_var, "") if self.api_token_env_var else None

            snapshot = _enumerate_compose(base_url, username, password, api_token, self.verify_ssl, self.projects)

            if self.workflow_selector is not None or self.data_mart_selector is not None:
                for proj in snapshot["projects"]:
                    if self.workflow_selector is not None:
                        proj["workflows"] = [w for w in proj["workflows"] if self.workflow_selector.matches(w["name"])]
                    if self.data_mart_selector is not None:
                        proj["data_marts"] = [m for m in proj["data_marts"] if self.data_mart_selector.matches(m["name"])]

            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())

            assets = []
            for proj in state.get("projects", []):
                pname = proj["name"]
                for w in proj.get("workflows", []):
                    assets.append(self._build_workflow_asset(pname, w["name"]))
                for m in proj.get("data_marts", []):
                    assets.append(self._build_data_mart_asset(pname, m["name"]))
            return dg.Definitions(assets=assets)

        def _build_workflow_asset(self, project: str, workflow: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, project, "workflow", workflow])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "qlik_project": dg.MetadataValue.text(project),
                    "qlik_workflow": dg.MetadataValue.text(workflow),
                    "compose_object": dg.MetadataValue.text("workflow"),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os, time
                try:
                    import requests
                except ImportError as e:
                    raise Exception("requests library not installed") from e

                base_url = os.environ.get(_self.base_url_env_var, "")
                username = os.environ.get(_self.username_env_var, "") if _self.username_env_var else None
                password = os.environ.get(_self.password_env_var, "") if _self.password_env_var else None
                api_token = os.environ.get(_self.api_token_env_var, "") if _self.api_token_env_var else None

                session = requests.Session()
                session.verify = _self.verify_ssl
                api_base = f"{base_url.rstrip('/')}/qlikcompose/api/v1"

                headers = {"Accept": "application/json", "Content-Type": "application/json"}
                if api_token:
                    headers["Authorization"] = f"Bearer {api_token}"
                else:
                    r = session.post(f"{api_base}/login", json={"username": username, "password": password}, timeout=30)
                    if r.status_code >= 300:
                        raise Exception(f"Compose login failed: {r.status_code}")

                workflow_url = f"{api_base}/projects/{project}/workflows/{workflow}"
                r = session.post(f"{workflow_url}?action=run", headers=headers, timeout=60)
                if r.status_code >= 400:
                    raise Exception(f"Compose workflow run failed: {r.status_code} {r.text[:200]}")
                context.log.info(f"Qlik Compose: run sent to {project}/{workflow}")

                if _self.wait_for_completion:
                    deadline = time.time() + _self.timeout_seconds
                    terminal = {"COMPLETED", "STOPPED", "ERROR", "FAILED"}
                    last_state = None
                    while time.time() < deadline:
                        time.sleep(_self.poll_interval_seconds)
                        sr = session.get(workflow_url, headers=headers, timeout=30)
                        if sr.status_code >= 300:
                            continue
                        body = sr.json() or {}
                        state = (body.get("workflow", {}) or {}).get("state") or body.get("state") or ""
                        if state and state != last_state:
                            context.log.info(f"workflow state: {state}")
                            last_state = state
                        if state and state.upper() in terminal:
                            if state.upper() in ("ERROR", "FAILED"):
                                raise Exception(f"Compose workflow ended in {state} ({project}/{workflow})")
                            context.add_output_metadata({"final_state": state})
                            return
                    raise Exception(f"Workflow did not reach terminal state within {_self.timeout_seconds}s ({project}/{workflow})")

            return _asset

        def _build_data_mart_asset(self, project: str, data_mart: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, project, "data_mart", data_mart])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "qlik_project": dg.MetadataValue.text(project),
                    "qlik_data_mart": dg.MetadataValue.text(data_mart),
                    "compose_object": dg.MetadataValue.text("data_mart"),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                # Data-mart assets are observational — record presence.
                # A separate qlik_compose_workflow_trigger_job can rebuild.
                context.log.info(f"Compose data mart {project}/{data_mart} present.")

            return _asset

else:
    class QlikComposeWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "QlikComposeWorkspaceComponent requires Dagster with StateBackedComponent support "
                "(post-2026 dagster>=1.11 or later)."
            )
