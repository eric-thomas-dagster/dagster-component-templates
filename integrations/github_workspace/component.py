"""GitHub Workspace Component.

Auto-enumerates repos × workflows in a GitHub org (or user) and emits one
Dagster asset per (repo, workflow). Materializing an asset reads the most
recent workflow runs and emits a DataFrame with their status and metadata.
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
class GitHubRepoSelector(dg.Resolvable):
    """Selector for filtering repos AND workflows. Fivetran-shape."""
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


def _enumerate_github(owner, token, verify_ssl, per_page=100) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    out: dict = {"repos": []}
    # Try org endpoint first; fall back to user endpoint.
    for endpoint in [f"https://api.github.com/orgs/{owner}/repos", f"https://api.github.com/users/{owner}/repos"]:
        try:
            r = session.get(endpoint, headers=headers, params={"per_page": per_page}, timeout=60)
            r.raise_for_status()
            body = r.json() or []
            if isinstance(body, list) and body:
                for repo in body:
                    name = repo.get("name")
                    if not name:
                        continue
                    workflows: list = []
                    try:
                        wr = session.get(
                            f"https://api.github.com/repos/{owner}/{name}/actions/workflows",
                            headers=headers, timeout=30,
                        )
                        wr.raise_for_status()
                        wbody = wr.json() or {}
                        for wf in wbody.get("workflows", []):
                            wname = wf.get("name") or wf.get("path", "")
                            wid = wf.get("id")
                            if wname and wid:
                                workflows.append({"id": wid, "name": wname, "path": wf.get("path", "")})
                    except Exception:  # noqa: BLE001
                        pass
                    out["repos"].append({"name": name, "workflows": workflows})
                break
        except Exception:  # noqa: BLE001
            continue
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class GitHubWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        owner_env_var: str  # e.g. GITHUB_OWNER: my-org
        token_env_var: str
        verify_ssl: bool = True

        repo_selector: Optional[GitHubRepoSelector] = None
        workflow_selector: Optional[GitHubRepoSelector] = None
        recent_runs_limit: int = 20

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["github"])
        compute_kind: str = "github"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"GitHubWorkspace[{hashlib.sha256(self.owner_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            owner = os.environ.get(self.owner_env_var, "")
            tok = os.environ.get(self.token_env_var, "")
            snapshot = _enumerate_github(owner, tok, self.verify_ssl)
            if self.repo_selector is not None:
                snapshot["repos"] = [r for r in snapshot["repos"] if self.repo_selector.matches(r["name"])]
            if self.workflow_selector is not None:
                for r in snapshot["repos"]:
                    r["workflows"] = [w for w in r["workflows"] if self.workflow_selector.matches(w["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for repo in state.get("repos", []):
                for wf in repo.get("workflows", []):
                    assets.append(self._build_asset(repo["name"], wf["id"], wf["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, repo_name: str, workflow_id: int, workflow_name: str):
            _self = self
            safe_wf = "".join(c if c.isalnum() or c == "_" else "_" for c in workflow_name)[:40] or f"wf_{workflow_id}"
            key = dg.AssetKey([*self.asset_key_prefix, repo_name, safe_wf])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "github_repo": dg.MetadataValue.text(repo_name),
                    "github_workflow_id": dg.MetadataValue.int(int(workflow_id)),
                    "github_workflow_name": dg.MetadataValue.text(workflow_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                owner = os.environ.get(_self.owner_env_var, "")
                tok = os.environ.get(_self.token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {
                    "Authorization": f"Bearer {tok}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                }

                url = f"https://api.github.com/repos/{owner}/{repo_name}/actions/workflows/{workflow_id}/runs"
                r = session.get(url, headers=headers, params={"per_page": _self.recent_runs_limit}, timeout=60)
                r.raise_for_status()
                body = r.json() or {}
                runs = body.get("workflow_runs", [])
                rows = [{
                    "id": run.get("id"),
                    "run_number": run.get("run_number"),
                    "status": run.get("status"),
                    "conclusion": run.get("conclusion"),
                    "event": run.get("event"),
                    "head_branch": run.get("head_branch"),
                    "actor": (run.get("actor") or {}).get("login"),
                    "created_at": run.get("created_at"),
                    "updated_at": run.get("updated_at"),
                } for run in runs]
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "github_repo": repo_name,
                    "github_workflow": workflow_name,
                })
                return df

            return _asset

else:
    class GitHubWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("GitHubWorkspaceComponent requires Dagster with StateBackedComponent support.")
