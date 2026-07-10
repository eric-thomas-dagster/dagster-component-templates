"""Qlik Replicate Workspace Component.

StateBackedComponent that auto-enumerates every Qlik Replicate task across
one or more Enterprise Manager servers and emits one Dagster asset per task.
The list of tasks is cached to disk on `write_state_to_path`; every subsequent
`build_defs_from_state` reads the cache without hitting the API — so cold
starts are fast and independent of Enterprise Manager availability.

Refresh the catalog explicitly via `dg utils refresh-defs-state` (or the
Dagster+ auto-refresh) — same pattern as the FivetranWorkspace shape.

Each emitted asset is materializable: it triggers the underlying Replicate
task (reload / resume) and polls until terminal state. So Dagster becomes
the imperative control plane over your entire Replicate fleet, with zero
per-task YAML.
"""
import hashlib
import json
import time
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


def _login_if_needed(session, base_url: str, username: Optional[str], password: Optional[str]) -> None:
    """POST /login for session-based auth. No-op for API-token flows."""
    if not (username and password):
        return
    api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
    r = session.post(
        f"{api_base}/login",
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code >= 300:
        raise Exception(f"Qlik EM login failed: {r.status_code} {r.text[:200]}")


def _api_headers(api_token: Optional[str]) -> dict:
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if api_token:
        h["Authorization"] = f"Bearer {api_token}"
    return h


def _enumerate_workspace(base_url: str, username: Optional[str], password: Optional[str],
                        api_token: Optional[str], verify_ssl: bool,
                        servers_filter: Optional[List[str]]) -> dict:
    """Return {servers: [{name, tasks: [{name, state, stage}]}]} from Qlik EM."""
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
    _login_if_needed(session, base_url, username, password)

    headers = _api_headers(api_token)

    servers: list = []
    r = session.get(f"{api_base}/servers", headers=headers, timeout=30)
    r.raise_for_status()
    server_body = r.json() or {}
    server_list = server_body.get("serverList") or server_body.get("servers") or []
    for sv in server_list:
        sv_name = sv.get("name") if isinstance(sv, dict) else str(sv)
        if not sv_name:
            continue
        if servers_filter is not None and sv_name not in servers_filter:
            continue

        tr = session.get(f"{api_base}/servers/{sv_name}/tasks", headers=headers, timeout=30)
        if tr.status_code >= 300:
            continue
        task_body = tr.json() or {}
        task_list = task_body.get("taskList") or task_body.get("tasks") or []

        server_tasks: list = []
        for t in task_list:
            t_name = t.get("name") if isinstance(t, dict) else str(t)
            if not t_name:
                continue
            # Fetch detail for state / stage.
            dr = session.get(f"{api_base}/servers/{sv_name}/tasks/{t_name}", headers=headers, timeout=15)
            detail = dr.json() or {} if dr.status_code < 300 else {}
            task_obj = detail.get("task") or detail
            server_tasks.append({
                "name": t_name,
                "state": task_obj.get("state"),
                "stage": task_obj.get("stage") or task_obj.get("current_stage"),
            })
        servers.append({"name": sv_name, "tasks": server_tasks})
    return {"servers": servers, "polled_at": time.time()}


@dataclass
class TaskSelector(dg.Resolvable):
    """Selector for filtering Qlik Replicate tasks.

    Mirrors the FivetranWorkspace `connector_selector` shape:

        task_selector:
          by_name: [orders_cdc, customers_cdc]       # exact names to include
          by_pattern: [orders_*]                      # globs to include
          exclude_by_name: [test_task]                # exact names to exclude
          exclude_by_pattern: [*_deprecated, *_test]  # globs to exclude

    Empty `by_name` + empty `by_pattern` = include all tasks.
    `exclude_by_*` always wins over `by_*`.
    """
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, task_name: str) -> bool:
        import fnmatch
        # Exclusions win.
        if self.exclude_by_name and task_name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(task_name, p) for p in self.exclude_by_pattern):
            return False
        # If no include filters, include everything.
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and task_name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(task_name, p) for p in self.by_pattern):
            return True
        return False


if _HAS_STATE_BACKED:

    @dataclass
    class QlikReplicateWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Auto-emit one Dagster asset per Qlik Replicate task.

        On `write_state_to_path`, enumerate every task across every server
        (optionally filtered) via the Qlik Enterprise Manager REST API. On
        `build_defs_from_state`, read the cached snapshot and emit one asset
        per (server, task). Materializing an asset triggers the underlying
        Replicate task and polls to completion.

        Example:

            ```yaml
            type: dagster_community_components.QlikReplicateWorkspaceComponent
            attributes:
              base_url_env_var: QLIK_EM_URL
              api_token_env_var: QLIK_EM_API_TOKEN
              servers: [prod-replicate-01]
              task_selector:
                by_name: [orders_sqlserver_to_snowflake]
              group_name: qlik_replicate
              action: reload           # what to do on materialize
              wait_for_completion: true
            ```
        """

        base_url_env_var: str
        username_env_var: Optional[str] = None
        password_env_var: Optional[str] = None
        api_token_env_var: Optional[str] = None
        verify_ssl: bool = True

        servers: Optional[List[str]] = None  # None = all servers
        task_selector: Optional[TaskSelector] = None  # None = all tasks

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["qlik_replicate"])
        compute_kind: str = "qlik_replicate"

        # What each asset does when materialized.
        action: str = "run"  # run | reload | stop
        run_option: str = "RESUME_PROCESSING"
        wait_for_completion: bool = True
        poll_interval_seconds: int = 15
        timeout_seconds: int = 3600

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"QlikReplicateWorkspace[{hashlib.sha256(self.base_url_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            base_url = os.environ.get(self.base_url_env_var, "")
            username = os.environ.get(self.username_env_var, "") if self.username_env_var else None
            password = os.environ.get(self.password_env_var, "") if self.password_env_var else None
            api_token = os.environ.get(self.api_token_env_var, "") if self.api_token_env_var else None
            snapshot = _enumerate_workspace(
                base_url=base_url, username=username, password=password, api_token=api_token,
                verify_ssl=self.verify_ssl, servers_filter=self.servers,
            )
            # Apply task_selector filtering.
            if self.task_selector is not None:
                for sv in snapshot["servers"]:
                    sv["tasks"] = [t for t in sv["tasks"] if self.task_selector.matches(t["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for sv in state.get("servers", []):
                sv_name = sv["name"]
                for t in sv["tasks"]:
                    t_name = t["name"]
                    assets.append(self._build_asset(sv_name, t_name, t))
            return dg.Definitions(assets=assets)

        def _build_asset(self, server: str, task: str, task_snapshot: dict):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, server, task])

            @dg.asset(
                key=key,
                group_name=self.group_name,
                compute_kind=self.compute_kind,
                metadata={
                    "qlik_server": dg.MetadataValue.text(server),
                    "qlik_task": dg.MetadataValue.text(task),
                    "state_at_discovery": dg.MetadataValue.text(str(task_snapshot.get("state"))),
                    "stage_at_discovery": dg.MetadataValue.text(str(task_snapshot.get("stage"))),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
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
                api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
                _login_if_needed(session, base_url, username, password)
                headers = _api_headers(api_token)

                task_url = f"{api_base}/servers/{server}/tasks/{task}"
                action_url = f"{task_url}?action={_self.action}"
                if _self.action == "run":
                    action_url += f"&option={_self.run_option}"

                r = session.post(action_url, headers=headers, timeout=60)
                if r.status_code >= 300:
                    raise Exception(
                        f"Qlik EM task action failed: {r.status_code} {r.text[:200]} "
                        f"(server={server} task={task} action={_self.action})"
                    )
                context.log.info(f"Qlik Replicate: {_self.action} sent to {server}/{task}")

                if _self.wait_for_completion:
                    deadline = time.time() + _self.timeout_seconds
                    terminal = {"STOPPED", "ERROR"}
                    last_state = None
                    while time.time() < deadline:
                        time.sleep(_self.poll_interval_seconds)
                        sr = session.get(task_url, headers=headers, timeout=30)
                        if sr.status_code >= 300:
                            continue
                        body = sr.json() or {}
                        state = (body.get("task", {}) or {}).get("state") or body.get("state")
                        if state and state != last_state:
                            context.log.info(f"task state: {state}")
                            last_state = state
                        if state and state.upper() in terminal:
                            if state.upper() == "ERROR":
                                raise Exception(f"Task ended in ERROR (server={server}, task={task})")
                            context.add_output_metadata({
                                "final_state": state,
                                "duration_seconds": round(time.time() - (deadline - _self.timeout_seconds), 2),
                            })
                            return
                    raise Exception(
                        f"Task did not reach terminal state within {_self.timeout_seconds}s "
                        f"(server={server}, task={task}, last state={last_state})"
                    )

            return _asset

else:  # StateBackedComponent unavailable (older Dagster)
    class QlikReplicateWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "QlikReplicateWorkspaceComponent requires Dagster with "
                "StateBackedComponent support (post-2026 dagster>=1.11 or later)."
            )
