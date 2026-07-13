"""Weights & Biases Workspace Component.

Auto-enumerates W&B projects for an entity via GraphQL (public API). Emits
one Dagster asset per project. Materializing fetches recent runs.
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
class WandbProjectSelector(dg.Resolvable):
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


def _enumerate_wandb(api_key, entity, base_url, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    session.auth = ("api", api_key)

    out: dict = {"projects": []}
    query = """
    query Entity($entityName: String!) {
      entity(name: $entityName) {
        projects(first: 500) {
          edges { node { name } }
        }
      }
    }
    """
    try:
        r = session.post(
            f"{base_url.rstrip('/')}/graphql",
            json={"query": query, "variables": {"entityName": entity}},
            timeout=60,
        )
        r.raise_for_status()
        body = r.json() or {}
        entity_node = ((body.get("data") or {}).get("entity") or {})
        for edge in (entity_node.get("projects") or {}).get("edges", []):
            n = ((edge or {}).get("node") or {}).get("name")
            if n:
                out["projects"].append({"name": n})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class WandbWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        api_key_env_var: str
        entity_env_var: str
        base_url: str = "https://api.wandb.ai"
        verify_ssl: bool = True

        project_selector: Optional[WandbProjectSelector] = None
        runs_limit: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["wandb"])
        compute_kind: str = "wandb"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"WandbWorkspace[{hashlib.sha256(self.api_key_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            key = os.environ.get(self.api_key_env_var, "")
            entity = os.environ.get(self.entity_env_var, "")
            snapshot = _enumerate_wandb(key, entity, self.base_url, self.verify_ssl)
            snapshot["entity"] = entity
            if self.project_selector is not None:
                snapshot["projects"] = [p for p in snapshot["projects"] if self.project_selector.matches(p["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            entity = state.get("entity", "")
            assets = []
            for p in state.get("projects", []):
                assets.append(self._build_asset(entity, p["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, entity: str, project_name: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, entity, project_name])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "wandb_entity": dg.MetadataValue.text(entity),
                    "wandb_project": dg.MetadataValue.text(project_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                api_key = os.environ.get(_self.api_key_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                session.auth = ("api", api_key)

                query = """
                query Project($entity: String!, $project: String!, $first: Int!) {
                  project(name: $project, entityName: $entity) {
                    runs(first: $first) {
                      edges { node { id name state createdAt summaryMetrics config } }
                    }
                  }
                }
                """
                r = session.post(
                    f"{_self.base_url.rstrip('/')}/graphql",
                    json={"query": query, "variables": {"entity": entity, "project": project_name, "first": _self.runs_limit}},
                    timeout=120,
                )
                r.raise_for_status()
                edges = (((r.json() or {}).get("data") or {}).get("project") or {}).get("runs", {}).get("edges", [])
                rows = []
                for edge in edges:
                    node = edge.get("node") or {}
                    rows.append({
                        "id": node.get("id"),
                        "name": node.get("name"),
                        "state": node.get("state"),
                        "created_at": node.get("createdAt"),
                        "summary_metrics": node.get("summaryMetrics"),
                        "config": node.get("config"),
                    })
                df = pd.DataFrame(rows)
                context.add_output_metadata({"row_count": len(df), "wandb_project": project_name})
                return df

            return _asset

else:
    class WandbWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("WandbWorkspaceComponent requires Dagster with StateBackedComponent support.")
