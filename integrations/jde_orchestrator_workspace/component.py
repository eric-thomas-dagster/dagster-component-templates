"""JDE Orchestrator Workspace Component.

StateBackedComponent that auto-enumerates every JDE orchestration and emits
one Dagster asset per orchestration. Discovery cached to disk on
`write_state_to_path`; every subsequent load reads the cache.
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
class OrchestrationSelector(dg.Resolvable):
    """Selector for filtering JDE orchestrations. Same shape as Fivetran's connector_selector."""
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


def _enumerate_jde(base_url, username, password, api_path_prefix, verify_ssl) -> dict:
    """Return {orchestrations: [{name}]}."""
    import base64
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}{api_path_prefix}"

    headers = {"Accept": "application/json"}
    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    out: dict = {"orchestrations": []}
    try:
        # GET /orchestrator returns the list of orchestrations in JDE Tools 9.2.7+
        r = session.get(f"{api_base}", headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json() or {}
        items = body.get("orchestrations") or body.get("value") or []
    except Exception:  # noqa: BLE001
        return out

    for item in items:
        name = item.get("name") if isinstance(item, dict) else str(item)
        if name:
            out["orchestrations"].append({"name": name})
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class JDEOrchestratorWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per JDE orchestration."""

        base_url_env_var: str
        username_env_var: str
        password_env_var: str
        api_path_prefix: str = "/jderest/v3/orchestrator"
        verify_ssl: bool = True

        orchestration_selector: Optional[OrchestrationSelector] = None

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["jde", "orchestration"])
        compute_kind: str = "jde"

        async_mode: bool = False
        wait_for_completion: bool = True
        poll_interval_seconds: int = 15
        timeout_seconds: int = 1800

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"JDEOrchestratorWorkspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            username = os.environ.get(self.username_env_var, "")
            password = os.environ.get(self.password_env_var, "")
            snapshot = _enumerate_jde(base_url, username, password, self.api_path_prefix, self.verify_ssl)
            if self.orchestration_selector is not None:
                snapshot["orchestrations"] = [o for o in snapshot["orchestrations"] if self.orchestration_selector.matches(o["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for o in state.get("orchestrations", []):
                assets.append(self._build_asset(o["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, orchestration: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, orchestration])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"jde_orchestration": dg.MetadataValue.text(orchestration)},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                import base64
                import time
                try:
                    import requests
                except ImportError as e:
                    raise Exception("requests library not installed") from e

                base_url = os.environ.get(_self.base_url_env_var, "")
                username = os.environ.get(_self.username_env_var, "")
                password = os.environ.get(_self.password_env_var, "")

                session = requests.Session()
                session.verify = _self.verify_ssl
                api_base = f"{base_url.rstrip('/')}{_self.api_path_prefix}"

                headers = {"Accept": "application/json", "Content-Type": "application/json"}
                if username and password:
                    token = base64.b64encode(f"{username}:{password}".encode()).decode()
                    headers["Authorization"] = f"Basic {token}"

                url = f"{api_base}/{orchestration}"
                if _self.async_mode:
                    url = f"{url}?asynchronous=true"

                r = session.post(url, json={}, headers=headers, timeout=_self.timeout_seconds)
                if r.status_code >= 400:
                    raise Exception(f"JDE orchestration failed: {r.status_code} {r.text[:200]} ({orchestration})")
                context.log.info(f"JDE: {orchestration} submitted (status={r.status_code})")

                if not (_self.async_mode and _self.wait_for_completion):
                    return

                try:
                    job_id = (r.json() or {}).get("jobId")
                except ValueError:
                    job_id = None
                if not job_id:
                    return

                deadline = time.time() + _self.timeout_seconds
                terminal = {"SUCCESS", "COMPLETED", "FAILED", "ERROR", "CANCELED"}
                while time.time() < deadline:
                    time.sleep(_self.poll_interval_seconds)
                    sr = session.get(f"{base_url.rstrip('/')}/jderest/v3/orchestrator/status/{job_id}", headers=headers, timeout=30)
                    if sr.status_code >= 300:
                        continue
                    state = ((sr.json() or {}).get("status") or "").upper()
                    if state in terminal:
                        if state in ("FAILED", "ERROR", "CANCELED"):
                            raise Exception(f"Orchestration ended in {state} ({orchestration})")
                        context.add_output_metadata({"final_state": state})
                        return
                raise Exception(f"Orchestration did not reach terminal state within {_self.timeout_seconds}s ({orchestration})")

            return _asset

else:
    class JDEOrchestratorWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "JDEOrchestratorWorkspaceComponent requires Dagster with StateBackedComponent support "
                "(post-2026 dagster>=1.11 or later)."
            )
