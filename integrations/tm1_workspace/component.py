"""TM1 Workspace Component.

StateBackedComponent that auto-enumerates every TM1 Cube, Process, and Chore
via the TM1 REST API and emits one Dagster asset per object. Discovery is
cached to disk on `write_state_to_path`; every subsequent `build_defs_from_state`
reads the cache without hitting the API.

Refresh the catalog via `dg utils refresh-defs-state` (or Dagster+ auto-refresh)
— same pattern as FivetranWorkspace.
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
class TM1ObjectSelector(dg.Resolvable):
    """Selector shape for filtering TM1 objects (Cubes / Processes / Chores).

    Mirrors FivetranWorkspace's `connector_selector` shape:

        cube_selector:
          by_name: [Sales, Finance]
          by_pattern: [Actual_*]
          exclude_by_name: [test_cube]
          exclude_by_pattern: [*_deprecated]

    Empty `by_name` + empty `by_pattern` = include everything.
    `exclude_by_*` always wins over `by_*`.
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


def _enumerate_tm1(base_url, username, password, cam_namespace, verify_ssl) -> dict:
    """Return {cubes: [...], processes: [...], chores: [...]}."""
    import base64
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/api/v1"

    headers = {"Accept": "application/json"}
    if cam_namespace:
        token = base64.b64encode(f"{username}:{password}:{cam_namespace}".encode()).decode()
        headers["Authorization"] = f"CAMNamespace {token}"
    else:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    out: dict = {"cubes": [], "processes": [], "chores": []}

    for kind, url_suffix, key in [
        ("cubes", "Cubes", "cubes"),
        ("processes", "Processes", "processes"),
        ("chores", "Chores", "chores"),
    ]:
        try:
            r = session.get(f"{api_base}/{url_suffix}", headers=headers, timeout=30)
            r.raise_for_status()
            body = r.json() or {}
            items = body.get("value") or []
            for item in items:
                name = item.get("Name")
                if not name:
                    continue
                out[key].append({"name": name})
        except Exception:  # noqa: BLE001 — enumeration best-effort per kind
            continue

    return out


if _HAS_STATE_BACKED:

    @dataclass
    class TM1WorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per TM1 Cube / Process / Chore.

        Example:

            ```yaml
            type: dagster_community_components.TM1WorkspaceComponent
            attributes:
              base_url_env_var: TM1_URL
              username_env_var: TM1_USER
              password_env_var: TM1_PASSWORD
              cube_selector:
                by_name: [Sales, Finance]
              process_selector:
                by_pattern: [Load_*]
              chore_selector:
                exclude_by_pattern: [*_deprecated]
              group_name: tm1_planning
              defs_state:
                management_type: LOCAL_FILESYSTEM
                refresh_if_dev: true
            ```
        """

        base_url_env_var: str
        username_env_var: str
        password_env_var: str
        cam_namespace_env_var: Optional[str] = None
        verify_ssl: bool = True

        cube_selector: Optional[TM1ObjectSelector] = None
        process_selector: Optional[TM1ObjectSelector] = None
        chore_selector: Optional[TM1ObjectSelector] = None

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["tm1"])
        compute_kind: str = "tm1"

        # For process / chore assets — action on materialize.
        wait_for_completion: bool = True
        timeout_seconds: int = 1800

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"TM1Workspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            username = os.environ.get(self.username_env_var, "")
            password = os.environ.get(self.password_env_var, "")
            cam_ns = os.environ.get(self.cam_namespace_env_var, "") if self.cam_namespace_env_var else None
            snapshot = _enumerate_tm1(base_url, username, password, cam_ns, self.verify_ssl)

            # Apply selectors.
            if self.cube_selector is not None:
                snapshot["cubes"] = [c for c in snapshot["cubes"] if self.cube_selector.matches(c["name"])]
            if self.process_selector is not None:
                snapshot["processes"] = [p for p in snapshot["processes"] if self.process_selector.matches(p["name"])]
            if self.chore_selector is not None:
                snapshot["chores"] = [c for c in snapshot["chores"] if self.chore_selector.matches(c["name"])]

            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())

            assets = []
            for c in state.get("cubes", []):
                assets.append(self._build_cube_asset(c["name"]))
            for p in state.get("processes", []):
                assets.append(self._build_process_asset(p["name"], target_type="process"))
            for c in state.get("chores", []):
                assets.append(self._build_process_asset(c["name"], target_type="chore"))
            return dg.Definitions(assets=assets)

        def _build_cube_asset(self, cube: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, "cube", cube])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"tm1_object": dg.MetadataValue.text(f"cube:{cube}")},
            )
            def _cube_asset(context: dg.AssetExecutionContext):
                # Cube assets are OBSERVATIONAL — materialize just records
                # that the cube exists; use tm1_cube_data_ingestion for
                # actual data extraction.
                context.log.info(f"TM1 cube {cube!r} present.")

            return _cube_asset

        def _build_process_asset(self, name: str, target_type: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, target_type, name])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"tm1_object": dg.MetadataValue.text(f"{target_type}:{name}")},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                import base64
                try:
                    import requests
                except ImportError as e:
                    raise Exception("requests library not installed") from e

                base_url = os.environ.get(_self.base_url_env_var, "")
                username = os.environ.get(_self.username_env_var, "")
                password = os.environ.get(_self.password_env_var, "")
                cam_ns = os.environ.get(_self.cam_namespace_env_var, "") if _self.cam_namespace_env_var else None

                session = requests.Session()
                session.verify = _self.verify_ssl
                api_base = f"{base_url.rstrip('/')}/api/v1"

                headers = {"Accept": "application/json", "Content-Type": "application/json"}
                if cam_ns:
                    token = base64.b64encode(f"{username}:{password}:{cam_ns}".encode()).decode()
                    headers["Authorization"] = f"CAMNamespace {token}"
                else:
                    token = base64.b64encode(f"{username}:{password}".encode()).decode()
                    headers["Authorization"] = f"Basic {token}"

                if target_type == "process":
                    exec_url = f"{api_base}/Processes('{name}')/tm1.ExecuteProcess"
                else:
                    exec_url = f"{api_base}/Chores('{name}')/tm1.Execute"

                r = session.post(exec_url, json={}, headers=headers, timeout=_self.timeout_seconds)
                if r.status_code >= 400:
                    raise Exception(
                        f"TM1 execute failed: {r.status_code} {r.text[:200]} ({target_type}:{name})"
                    )
                context.log.info(f"TM1: {target_type} {name!r} executed (status={r.status_code})")

                try:
                    body = r.json() or {}
                    status = body.get("ProcessExecuteStatusCode") or body.get("Status")
                    if status and status not in ("CompletedSuccessfully", "Success"):
                        raise Exception(f"TM1 {target_type} ended with status {status}")
                    if status:
                        context.add_output_metadata({"tm1_status": status})
                except ValueError:
                    pass  # bare 200 is fine

            return _asset

else:  # StateBackedComponent unavailable
    class TM1WorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "TM1WorkspaceComponent requires Dagster with StateBackedComponent "
                "support (post-2026 dagster>=1.11 or later)."
            )
