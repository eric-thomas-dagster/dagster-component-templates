"""Jira Workspace Component.

Auto-enumerates Jira projects; emits one Dagster asset per project. Materializing
an asset fetches issues in that project via JQL.
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
class JiraProjectSelector(dg.Resolvable):
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


def _auth_header(email: str, api_token: str) -> str:
    return "Basic " + base64.b64encode(f"{email}:{api_token}".encode()).decode()


def _enumerate_projects(base_url, email, api_token, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": _auth_header(email, api_token), "Accept": "application/json"}

    out: dict = {"projects": []}
    try:
        r = session.get(f"{base_url.rstrip('/')}/rest/api/3/project/search", headers=headers, timeout=60)
        r.raise_for_status()
        body = r.json() or {}
        for p in body.get("values", []):
            key = p.get("key")
            name = p.get("name", key)
            if key:
                out["projects"].append({"key": key, "name": name})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class JiraWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        base_url_env_var: str  # e.g. JIRA_URL: https://acme.atlassian.net
        email_env_var: str
        api_token_env_var: str
        verify_ssl: bool = True

        project_selector: Optional[JiraProjectSelector] = None
        jql_template: str = "project = {project_key} ORDER BY created DESC"
        max_results: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["jira", "project"])
        compute_kind: str = "jira"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"JiraWorkspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            email = os.environ.get(self.email_env_var, "")
            tok = os.environ.get(self.api_token_env_var, "")
            snapshot = _enumerate_projects(base_url, email, tok, self.verify_ssl)
            if self.project_selector is not None:
                snapshot["projects"] = [
                    p for p in snapshot["projects"]
                    if self.project_selector.matches(p["key"]) or self.project_selector.matches(p["name"])
                ]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for p in state.get("projects", []):
                assets.append(self._build_asset(p["key"], p["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, project_key: str, project_name: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, project_key])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "jira_project_key": dg.MetadataValue.text(project_key),
                    "jira_project_name": dg.MetadataValue.text(project_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                base_url = os.environ.get(_self.base_url_env_var, "")
                email = os.environ.get(_self.email_env_var, "")
                tok = os.environ.get(_self.api_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": _auth_header(email, tok), "Accept": "application/json"}

                jql = _self.jql_template.format(project_key=project_key)
                r = session.get(
                    f"{base_url.rstrip('/')}/rest/api/3/search",
                    headers=headers,
                    params={"jql": jql, "maxResults": _self.max_results},
                    timeout=120,
                )
                r.raise_for_status()
                body = r.json() or {}
                issues = body.get("issues", [])
                rows = []
                for iss in issues:
                    fields = iss.get("fields") or {}
                    rows.append({
                        "key": iss.get("key"),
                        "summary": fields.get("summary"),
                        "status": (fields.get("status") or {}).get("name"),
                        "priority": (fields.get("priority") or {}).get("name"),
                        "assignee": (fields.get("assignee") or {}).get("displayName"),
                        "created": fields.get("created"),
                        "updated": fields.get("updated"),
                    })
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "project_key": project_key,
                    "jql": jql,
                })
                return df

            return _asset

else:
    class JiraWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("JiraWorkspaceComponent requires Dagster with StateBackedComponent support.")
