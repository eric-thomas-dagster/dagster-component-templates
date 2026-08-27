"""InformaticaWorkspaceComponent.

Wrap Informatica IDMC (Intelligent Data Management Cloud, formerly IICS)
behind a Dagster workspace-shape component. Discovers every task in the
IDMC org and emits one AssetSpec per task. On materialize, POSTs the task
via `/api/v2/job` and polls `/api/v2/activity/activityLog/{runId}` until
the task finishes.

Backing REST (IDMC public API v3 + legacy v2):

    POST /public/core/v3/login              — session ticket + POD URL
    GET  /public/core/v3/objects            — task/mtt enumeration
    POST /api/v2/job                        — trigger a task run
                                                (v2 API is stable + widely deployed)
    GET  /api/v2/activity/activityMonitor   — running/queued jobs
    GET  /api/v2/activity/activityLog       — historical run log

Auth pattern is two-step (unlike Talend's static bearer):

    1. POST {login_url}/public/core/v3/login with {username, password}
       Response includes:
          - `userInfo.sessionId`   (session ticket)
          - `products[].baseApiUrl` (POD-specific URL for subsequent calls)

    2. All subsequent calls use `Authorization: INFA-SESSION <sessionId>`
       against the `baseApiUrl` returned in step 1.

Session tickets expire after ~30 min of inactivity; this component
re-logs-in per code-location load + per sensor tick.

IDMC task run status values (from activityLog[].runStatus):
    RUNNING            job in progress
    SUCCESS            completed successfully
    FAILED             completed with errors
    STOPPED            user- or system-canceled
    QUEUED             waiting for a Secure Agent
    WARNING            completed with warnings (treated as SUCCESS-ish)
"""

from __future__ import annotations

import fnmatch
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field


IDMC_TERMINAL: set = {"SUCCESS", "FAILED", "STOPPED", "WARNING"}
IDMC_SUCCESS: set = {"SUCCESS", "WARNING"}


class InformaticaWorkspaceConfig(dg.Model):
    """IDMC connection.

    IDMC uses a two-step login flow: authenticate against the regional
    login URL, get back a session ID + POD-specific base API URL. All
    subsequent calls hit the POD URL with `Authorization: INFA-SESSION
    <sessionId>`.
    """

    login_url: str = Field(
        default="https://dm-us.informaticacloud.com",
        description=(
            "IDMC regional login URL. Common values: "
            "`https://dm-us.informaticacloud.com` (US), "
            "`https://dm-em.informaticacloud.com` (EU), "
            "`https://dm-ap.informaticacloud.com` (AP)."
        ),
    )
    username: Optional[str] = Field(
        default=None,
        description="IDMC username. Prefer `username_env_var` for secrets.",
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the IDMC username.",
    )
    password: Optional[str] = Field(
        default=None,
        description="IDMC password. Prefer `password_env_var` for secrets.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the IDMC password.",
    )
    request_timeout_seconds: int = Field(default=60)
    verify_ssl: bool = Field(default=True)


@dataclass
class InformaticaTask:
    """One task from IDMC's /objects enumeration."""
    id: str                    # Global ID
    name: str
    kind: str                  # 'MTT' | 'DMASK' | 'DPS' | 'DRS' | 'MI_TASK' | ...
    folder_id: Optional[str]
    folder_path: Optional[str]
    description: Optional[str]

    @property
    def qualified_name(self) -> str:
        return f"{self.folder_path or 'root'}/{self.name}" if self.folder_path else self.name


class InformaticaTaskSelector(dg.Model):
    """Filter which IDMC tasks become Dagster assets."""

    by_kind: Optional[List[str]] = Field(
        default=None,
        description="Restrict by task type. Common values: `MTT` (mapping "
        "task), `DMASK` (data masking), `DPS` (data processing service), "
        "`DRS` (data replication service), `MI_TASK` (mass ingestion). "
        "Default: all task types.",
    )
    by_folder_path: Optional[List[str]] = Field(
        default=None,
        description="Restrict by exact folder path prefix.",
    )
    include: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns against `<folder_path>/<name>` (case-insensitive).",
    )
    exclude: Optional[List[str]] = Field(
        default=None, description="fnmatch patterns to EXCLUDE. Applied last."
    )


class InformaticaWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Informatica IDMC (IICS) tasks as Dagster assets.

    Example:

    ```yaml
    type: dagster_community_components.InformaticaWorkspaceComponent
    attributes:
      workspace:
        login_url:        https://dm-us.informaticacloud.com
        username_env_var: IDMC_USERNAME
        password_env_var: IDMC_PASSWORD
      task_selector:
        by_kind: [MTT]                          # mapping tasks only
        by_folder_path: ["Sales/Prod"]
        include: ["*etl*"]
        exclude: ["*_test*"]
      action: execute
      wait_for_completion: true
      poll_interval_seconds: 30
      timeout_seconds: 3600
      polling_sensor: true
      observation_interval_seconds: 300
      freshness_lag_threshold_seconds: 3600
      group_name: informatica_prod
      kinds: [informatica, idmc, etl]
    ```
    """

    workspace: InformaticaWorkspaceConfig = Field(
        description="IDMC connection details."
    )
    task_selector: Optional[InformaticaTaskSelector] = Field(
        default=None,
        description="Filter which tasks become assets. Default = all.",
    )
    action: str = Field(
        default="noop",
        description=(
            "materialize() behavior. `noop` = external asset. `execute` = "
            "POST /api/v2/job + poll /api/v2/activity/activityLog."
        ),
    )
    wait_for_completion: bool = Field(default=True)
    poll_interval_seconds: int = Field(default=30)
    timeout_seconds: int = Field(default=3600)
    polling_sensor: bool = Field(default=False)
    observation_interval_seconds: int = Field(default=300)
    freshness_lag_threshold_seconds: Optional[int] = Field(default=None)
    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description="Default: `['informatica', 'idmc']`.",
    )
    group_name: Optional[str] = Field(default="informatica_idmc")
    kinds: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    # ── Auth ──────────────────────────────────────────────────────
    def _resolve_creds(self) -> Tuple[str, str]:
        import os
        cfg = self.workspace
        user = cfg.username or (
            os.environ.get(cfg.username_env_var, "") if cfg.username_env_var else ""
        )
        password = cfg.password or (
            os.environ.get(cfg.password_env_var, "") if cfg.password_env_var else ""
        )
        if not (user and password):
            raise ValueError(
                "InformaticaWorkspaceComponent.workspace: supply "
                "username/password OR username_env_var/password_env_var."
            )
        return user, password

    def _login(self) -> Tuple[str, str]:
        """POST /public/core/v3/login. Returns (session_id, base_api_url)."""
        import requests
        user, password = self._resolve_creds()
        r = requests.post(
            f"{self.workspace.login_url.rstrip('/')}/public/core/v3/login",
            json={"username": user, "password": password},
            timeout=self.workspace.request_timeout_seconds,
            verify=self.workspace.verify_ssl,
        )
        r.raise_for_status()
        payload = r.json()
        session_id = (payload.get("userInfo") or {}).get("sessionId", "")
        products = payload.get("products") or []
        base_url = (products[0].get("baseApiUrl") if products else "") or ""
        if not (session_id and base_url):
            raise RuntimeError(
                f"IDMC login: missing sessionId/baseApiUrl in response: {payload}"
            )
        return session_id, base_url.rstrip("/")

    def _auth_headers(self, session_id: str) -> Dict[str, str]:
        return {
            "INFA-SESSION-ID": session_id,
            "Accept": "application/json",
        }

    def _http_get(self, path: str, session_id: str, base_url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        import requests
        r = requests.get(
            f"{base_url}{path}",
            headers=self._auth_headers(session_id),
            params=params or {},
            timeout=self.workspace.request_timeout_seconds,
            verify=self.workspace.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def _http_post(self, path: str, session_id: str, base_url: str, json_body: Dict[str, Any]) -> Any:
        import requests
        r = requests.post(
            f"{base_url}{path}",
            headers={**self._auth_headers(session_id), "Content-Type": "application/json"},
            json=json_body,
            timeout=self.workspace.request_timeout_seconds,
            verify=self.workspace.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def _discover_tasks(self) -> Tuple[List[InformaticaTask], str, str]:
        """POST /login → GET /objects. Returns (tasks, session_id, base_url)."""
        session_id, base_url = self._login()
        # Enumerate objects — IDMC's /objects endpoint supports type filters.
        # For MVP we grab all task-shaped objects; selector filters afterward.
        params = {
            "q": "type=='MTT'||type=='DMASK'||type=='DPS'||type=='DRS'||type=='MI_TASK'",
            "limit": 200,
        }
        data = self._http_get("/public/core/v3/objects", session_id, base_url, params=params)
        objects = data.get("objects", []) if isinstance(data, dict) else (data or [])

        tasks: List[InformaticaTask] = []
        for obj in objects:
            tasks.append(
                InformaticaTask(
                    id=str(obj.get("id") or ""),
                    name=str(obj.get("path", "").split("/")[-1] or obj.get("name") or ""),
                    kind=str(obj.get("type") or "UNKNOWN").upper(),
                    folder_id=obj.get("folderId"),
                    folder_path=obj.get("path", "").rsplit("/", 1)[0] if obj.get("path") else None,
                    description=obj.get("description"),
                )
            )

        sel = self.task_selector
        if sel:
            def _match(t: InformaticaTask) -> bool:
                if sel.by_kind and t.kind.upper() not in [k.upper() for k in sel.by_kind]:
                    return False
                if sel.by_folder_path and not any(
                    (t.folder_path or "").startswith(fp) for fp in sel.by_folder_path
                ):
                    return False
                qname = t.qualified_name.lower()
                if sel.include and not any(
                    fnmatch.fnmatch(qname, pat.lower()) for pat in sel.include
                ):
                    return False
                if sel.exclude and any(
                    fnmatch.fnmatch(qname, pat.lower()) for pat in sel.exclude
                ):
                    return False
                return True

            tasks = [t for t in tasks if _match(t)]

        return tasks, session_id, base_url

    def _execute_task(self, task: InformaticaTask, session_id: str, base_url: str, context) -> Dict[str, Any]:
        """POST /api/v2/job + poll /api/v2/activity/activityLog to completion."""
        # v2 API uses the same session ID; body is legacy-shape.
        body = {"taskId": task.id, "taskType": task.kind}
        r = self._http_post("/api/v2/job", session_id, base_url, body)
        run_id = str(r.get("runId") or r.get("jobRunId") or "")
        if not run_id:
            raise RuntimeError(
                f"POST /api/v2/job for {task.qualified_name} returned no runId: {r}"
            )
        context.log.info(
            f"IDMC task {task.qualified_name} triggered — run_id={run_id}"
        )

        if not self.wait_for_completion:
            return {"run_id": run_id, "run_status": None, "error_message": None}

        start = time.time()
        while True:
            log = self._http_get(
                "/api/v2/activity/activityLog",
                session_id,
                base_url,
                params={"runId": run_id},
            )
            entries = log if isinstance(log, list) else log.get("activityLog", []) or []
            if entries:
                # Most recent entry — pick the top row.
                entry = entries[0]
                status = str(entry.get("runStatus") or entry.get("state") or "").upper()
                if status in IDMC_TERMINAL:
                    return {
                        "run_id": run_id,
                        "run_status": status,
                        "error_message": entry.get("errorMessage") or None,
                        "end_time": entry.get("endTime"),
                    }
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise TimeoutError(
                    f"IDMC task {task.qualified_name} exceeded timeout of "
                    f"{self.timeout_seconds}s"
                )
            time.sleep(self.poll_interval_seconds)

    # ── build_defs ────────────────────────────────────────────────
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        try:
            tasks, _sid, _base = self._discover_tasks()
        except Exception as e:
            import warnings
            warnings.warn(
                f"InformaticaWorkspaceComponent: could not discover tasks "
                f"({type(e).__name__}: {e}). Emitting empty definitions."
            )
            return dg.Definitions()

        prefix = self.asset_key_prefix or ["informatica", "idmc"]
        kinds = self.kinds or ["informatica", "idmc"]

        specs: List[dg.AssetSpec] = []
        for t in tasks:
            folder_bits = [p for p in (t.folder_path or "").split("/") if p]
            key_path = [*prefix, *folder_bits, t.name] if folder_bits else [*prefix, t.name]
            spec = dg.AssetSpec(
                key=dg.AssetKey(key_path),
                description=t.description or f"IDMC {t.kind} `{t.qualified_name}`",
                group_name=self.group_name,
                kinds=set(kinds),
                tags=self.tags or {},
                owners=self.owners or [],
                metadata={
                    "informatica/id": t.id,
                    "informatica/name": t.name,
                    "informatica/kind": t.kind,
                    "informatica/folder_path": t.folder_path or "",
                },
            )
            specs.append(spec)

        action = (self.action or "noop").lower()

        if action == "noop":
            defs_kwargs: Dict[str, Any] = {"assets": specs}
        elif action == "execute":
            @dg.multi_asset(specs=specs)
            def _informatica_execute(context: dg.AssetExecutionContext):
                # Re-login per run for freshness — session tickets can time out
                # between load-time discovery and run-time execution.
                session_id, base_url = _self._login()
                for spec in specs:
                    task = InformaticaTask(
                        id=spec.metadata["informatica/id"],
                        name=spec.metadata["informatica/name"],
                        kind=spec.metadata["informatica/kind"],
                        folder_id=None,
                        folder_path=spec.metadata.get("informatica/folder_path") or None,
                        description=None,
                    )
                    result = _self._execute_task(task, session_id, base_url, context)
                    if _self.wait_for_completion and result.get("run_status") not in IDMC_SUCCESS:
                        raise dg.Failure(
                            description=(
                                f"IDMC {task.qualified_name} finished with "
                                f"status={result.get('run_status')!r}: "
                                f"{result.get('error_message')}"
                            )
                        )
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "informatica/run_id": result["run_id"],
                            "informatica/run_status": result.get("run_status") or "async",
                        },
                    )

            defs_kwargs = {"assets": [_informatica_execute]}
        else:
            raise ValueError(
                f"InformaticaWorkspaceComponent.action={action!r} not supported. "
                f"Use 'noop' or 'execute'."
            )

        if self.polling_sensor and specs:
            @dg.sensor(
                name="informatica_workspace_observation_sensor",
                minimum_interval_seconds=self.observation_interval_seconds,
                default_status=dg.DefaultSensorStatus.STOPPED,
                asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
            )
            def _observation_sensor(context: dg.SensorEvaluationContext):
                cursor_val = context.cursor or ""
                sid, base = _self._login()
                data = _self._http_get(
                    "/api/v2/activity/activityLog",
                    sid,
                    base,
                    params={"top": 100},
                )
                entries = data if isinstance(data, list) else data.get("activityLog", []) or []

                observations = []
                new_cursor = cursor_val
                for entry in entries:
                    ts = entry.get("endTime") or entry.get("startTime") or ""
                    if cursor_val and ts <= cursor_val:
                        continue
                    status = str(entry.get("runStatus") or "").upper()
                    if status not in IDMC_TERMINAL:
                        continue
                    new_cursor = max(new_cursor, ts) if new_cursor else ts
                    task_name = entry.get("objectName") or entry.get("taskName") or ""
                    task_kind = str(entry.get("taskType") or entry.get("assetType") or "").upper()
                    matching_specs = [
                        s for s in specs
                        if s.metadata["informatica/name"] == task_name
                        and s.metadata["informatica/kind"] == task_kind
                    ]
                    for s in matching_specs:
                        observations.append(
                            dg.AssetObservation(
                                asset_key=s.key,
                                metadata={
                                    "informatica/run_id": entry.get("runId") or "",
                                    "informatica/run_status": status,
                                    "informatica/end_time": ts,
                                },
                            )
                        )
                return dg.SensorResult(
                    asset_events=observations,
                    cursor=str(new_cursor) if new_cursor else "",
                )

            defs_kwargs["sensors"] = [_observation_sensor]

        if self.freshness_lag_threshold_seconds is not None and specs:
            from datetime import datetime, timezone
            threshold = self.freshness_lag_threshold_seconds

            checks = []
            for spec in specs:
                @dg.asset_check(
                    asset=spec.key,
                    name="informatica_freshness_lag",
                    description=(
                        f"Fails when the last successful IDMC run of this task "
                        f"is older than {threshold}s."
                    ),
                )
                def _check(_key=spec.key):
                    task_name = next(
                        s.metadata["informatica/name"] for s in specs if s.key == _key
                    )
                    task_kind = next(
                        s.metadata["informatica/kind"] for s in specs if s.key == _key
                    )
                    sid, base = _self._login()
                    data = _self._http_get(
                        "/api/v2/activity/activityLog",
                        sid,
                        base,
                        params={"top": 50},
                    )
                    entries = data if isinstance(data, list) else data.get("activityLog", []) or []
                    latest = None
                    for e in entries:
                        if (
                            (e.get("objectName") == task_name or e.get("taskName") == task_name)
                            and str(e.get("taskType") or e.get("assetType") or "").upper() == task_kind
                            and str(e.get("runStatus") or "").upper() in IDMC_SUCCESS
                        ):
                            latest = e
                            break
                    if not latest:
                        return dg.AssetCheckResult(
                            passed=False, description="No successful runs found."
                        )
                    end_time_raw = latest.get("endTime")
                    if not end_time_raw:
                        return dg.AssetCheckResult(
                            passed=False, description="Most recent run has no endTime."
                        )
                    end_time = datetime.fromisoformat(str(end_time_raw).replace("Z", "+00:00"))
                    lag = (datetime.now(timezone.utc) - end_time).total_seconds()
                    return dg.AssetCheckResult(
                        passed=lag <= threshold,
                        description=f"lag={int(lag)}s (threshold={threshold}s)",
                        metadata={
                            "informatica/last_success_at": str(end_time),
                            "informatica/lag_seconds": int(lag),
                        },
                    )
                checks.append(_check)

            defs_kwargs["asset_checks"] = checks

        return dg.Definitions(**defs_kwargs)
