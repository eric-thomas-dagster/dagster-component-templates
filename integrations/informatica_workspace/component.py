"""InformaticaWorkspaceComponent.

Wrap Informatica IDMC (Intelligent Data Management Cloud, formerly IICS)
behind a Dagster workspace-shape component. Discovers every task in the
IDMC org and emits one AssetSpec per task. On materialize, POSTs the task
via `/api/v2/job` and polls `/api/v2/activity/activityLog/{runId}` until
the task finishes.

Full workspace-pattern shape (parity with hvr_hub_workspace / snowflake_workspace):
  - `@public` class annotation
  - `@record` props class
  - `translation:` callable for per-asset customization
  - `StateBackedComponent` inheritance — discovery cached to disk via
    `write_state_to_path`. Refresh via `dg utils refresh-defs-state`.

Backing REST (IDMC public API v3 + legacy v2):

    POST /public/core/v3/login              — session ticket + POD URL
    GET  /public/core/v3/objects            — task/mtt enumeration
    POST /api/v2/job                        — trigger a task run
    GET  /api/v2/activity/activityLog       — historical run log

Auth is two-step: log in with username/password against a regional login
URL, get back a sessionId + POD-specific base URL. Subsequent calls use
`INFA-SESSION-ID: <sessionId>` against the POD URL.
"""

import fnmatch
import hashlib
import json
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Tuple

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    TranslationFn,
    TranslationFnResolver,
)
from dagster_shared.record import record
from pydantic import Field


IDMC_TERMINAL: set = {"SUCCESS", "FAILED", "STOPPED", "WARNING"}
IDMC_SUCCESS: set = {"SUCCESS", "WARNING"}


# ── Props (@record) for translator callable ─────────────────────────
@record
class InformaticaTaskProps:
    """Data passed to `translation:` callables for each IDMC task."""

    id: str
    name: str
    kind: str
    folder_id: Optional[str] = None
    folder_path: Optional[str] = None
    description: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        return f"{self.folder_path or 'root'}/{self.name}" if self.folder_path else self.name


# ── Workspace config nested block ───────────────────────────────────
class InformaticaWorkspaceConfig(dg.Model):
    """IDMC connection.

    Two-step auth: log in with username/password → get sessionId + baseApiUrl
    → all subsequent calls use `INFA-SESSION-ID: <sessionId>` against the POD
    URL. Session tickets expire after ~30 min idle; component re-logs-in per
    code-location load + per sensor tick + per materialization run.
    """

    login_url: str = Field(
        default="https://dm-us.informaticacloud.com",
        description=(
            "IDMC regional login URL. `https://dm-us.informaticacloud.com` (US), "
            "`https://dm-em.informaticacloud.com` (EU), "
            "`https://dm-ap.informaticacloud.com` (AP)."
        ),
    )
    username: Optional[str] = Field(
        default=None, description="IDMC username. Prefer `username_env_var` for secrets."
    )
    username_env_var: Optional[str] = Field(
        default=None, description="Env var holding the IDMC username."
    )
    password: Optional[str] = Field(
        default=None, description="IDMC password. Prefer `password_env_var` for secrets."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var holding the IDMC password."
    )
    request_timeout_seconds: int = Field(default=60)
    verify_ssl: bool = Field(default=True)


# ── Selector block ──────────────────────────────────────────────────
class InformaticaTaskSelector(dg.Model):
    """Filter which IDMC tasks become Dagster assets."""

    by_kind: Optional[List[str]] = Field(
        default=None,
        description="Restrict by task type: `MTT` / `DMASK` / `DPS` / `DRS` / `MI_TASK`.",
    )
    by_folder_path: Optional[List[str]] = Field(
        default=None, description="Restrict by exact folder path prefix."
    )
    include: Optional[List[str]] = Field(
        default=None, description="fnmatch patterns against `<folder_path>/<name>`."
    )
    exclude: Optional[List[str]] = Field(
        default=None, description="fnmatch patterns to EXCLUDE. Applied last."
    )


# ── Base translator ─────────────────────────────────────────────────
class InformaticaComponentTranslator:
    """Base translator: InformaticaTaskProps → AssetSpec."""

    def __init__(self, component: "InformaticaWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: InformaticaTaskProps) -> AssetSpec:
        prefix = self._component.asset_key_prefix or ["informatica", "idmc"]
        folder_bits = [p for p in (props.folder_path or "").split("/") if p]
        key_path = [*prefix, *folder_bits, props.name] if folder_bits else [*prefix, props.name]
        return AssetSpec(
            key=AssetKey(key_path),
            description=props.description or f"IDMC {props.kind} `{props.qualified_name}`",
            group_name=self._component.group_name,
            # Dagster kinds — Informatica has no first-class icon in Dagster,
            # so these render as text-only badges. Still useful for catalog
            # filtering (`kind:etl`, `kind:informatica`, `kind:idmc`).
            kinds=set(self._component.kinds or ["informatica", "idmc", "etl"]),
            tags=dict(self._component.tags or {}),
            owners=list(self._component.owners or []),
            metadata={
                "informatica/id": props.id,
                "informatica/name": props.name,
                "informatica/kind": props.kind,
                "informatica/folder_path": props.folder_path or "",
            },
        )


# ── Component ───────────────────────────────────────────────────────
@public
class InformaticaWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Informatica IDMC (IICS) tasks as Dagster assets — full workspace-pattern shape.

    Example:

    ```yaml
    type: dagster_community_components.InformaticaWorkspaceComponent
    attributes:
      workspace:
        login_url:        https://dm-us.informaticacloud.com
        username_env_var: IDMC_USERNAME
        password_env_var: IDMC_PASSWORD
      task_selector:
        by_kind: [MTT]
        by_folder_path: ["Sales/Prod"]
        include: ["*etl*"]
        exclude: ["*_test*"]
      # translation: |
      #   {{ load_python_module_attr('my_project.idmc.translate.by_folder') }}
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

    workspace: InformaticaWorkspaceConfig = Field(description="IDMC connection details.")
    task_selector: Optional[InformaticaTaskSelector] = Field(default=None)
    translation: Annotated[
        Optional[TranslationFn[InformaticaTaskProps]],
        TranslationFnResolver(
            template_vars_for_translation_fn=lambda data: {"props": data}
        ),
    ] = Field(
        default=None,
        description=(
            "Optional per-asset translation callable. Receives an "
            "InformaticaTaskProps and returns AssetSpec overrides. Use for "
            "per-folder / per-kind customization."
        ),
    )
    action: str = Field(default="noop")
    wait_for_completion: bool = Field(default=True)
    poll_interval_seconds: int = Field(default=30)
    timeout_seconds: int = Field(default=3600)
    polling_sensor: bool = Field(default=False)
    observation_interval_seconds: int = Field(default=300)
    freshness_lag_threshold_seconds: Optional[int] = Field(default=None)
    asset_key_prefix: Optional[List[str]] = Field(
        default=None, description="Default: `['informatica', 'idmc']`."
    )
    group_name: Optional[str] = Field(default="informatica_idmc")
    kinds: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description="StateBackedComponent state config. Default: local filesystem cache.",
    )

    @property
    def _base_translator(self) -> InformaticaComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = InformaticaComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @public
    def get_asset_spec(self, props: InformaticaTaskProps) -> AssetSpec:
        base_spec = self._base_translator.get_asset_spec(props)
        if self.translation is None:
            return base_spec
        overrides = self.translation(base_spec, props) or {}
        if isinstance(overrides, AssetSpec):
            return overrides
        return base_spec._replace(**overrides) if hasattr(base_spec, "_replace") else base_spec

    @property
    def defs_state_config(self) -> DefsStateConfig:
        composite = f"{self.workspace.login_url}::idmc"
        state_hash = hashlib.sha256(composite.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{state_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

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
            raise RuntimeError(f"IDMC login: missing sessionId/baseApiUrl: {payload}")
        return session_id, base_url.rstrip("/")

    def _auth_headers(self, session_id: str) -> Dict[str, str]:
        return {"INFA-SESSION-ID": session_id, "Accept": "application/json"}

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

    def _discover_tasks(self) -> List[Dict[str, Any]]:
        session_id, base_url = self._login()
        params = {
            "q": "type=='MTT'||type=='DMASK'||type=='DPS'||type=='DRS'||type=='MI_TASK'",
            "limit": 200,
        }
        data = self._http_get("/public/core/v3/objects", session_id, base_url, params=params)
        objects = data.get("objects", []) if isinstance(data, dict) else (data or [])

        tasks: List[Dict[str, Any]] = []
        for obj in objects:
            tasks.append({
                "id": str(obj.get("id") or ""),
                "name": str(obj.get("path", "").split("/")[-1] or obj.get("name") or ""),
                "kind": str(obj.get("type") or "UNKNOWN").upper(),
                "folder_id": obj.get("folderId"),
                "folder_path": obj.get("path", "").rsplit("/", 1)[0] if obj.get("path") else None,
                "description": obj.get("description"),
            })

        sel = self.task_selector
        if not sel:
            return tasks

        def _match(t: Dict[str, Any]) -> bool:
            if sel.by_kind and t["kind"].upper() not in [k.upper() for k in sel.by_kind]:
                return False
            if sel.by_folder_path and not any(
                (t.get("folder_path") or "").startswith(fp) for fp in sel.by_folder_path
            ):
                return False
            qname = f"{t.get('folder_path') or 'root'}/{t['name']}".lower()
            if sel.include and not any(
                fnmatch.fnmatch(qname, pat.lower()) for pat in sel.include
            ):
                return False
            if sel.exclude and any(
                fnmatch.fnmatch(qname, pat.lower()) for pat in sel.exclude
            ):
                return False
            return True

        return [t for t in tasks if _match(t)]

    def _execute_task(self, task: Dict[str, Any], session_id: str, base_url: str, context) -> Dict[str, Any]:
        body = {"taskId": task["id"], "taskType": task["kind"]}
        r = self._http_post("/api/v2/job", session_id, base_url, body)
        run_id = str(r.get("runId") or r.get("jobRunId") or "")
        if not run_id:
            raise RuntimeError(
                f"POST /api/v2/job for {task.get('name')} returned no runId: {r}"
            )
        context.log.info(f"IDMC task {task.get('name')} triggered — run_id={run_id}")

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
                entry = entries[0]
                status = str(entry.get("runStatus") or entry.get("state") or "").upper()
                if status in IDMC_TERMINAL:
                    return {
                        "run_id": run_id,
                        "run_status": status,
                        "error_message": entry.get("errorMessage") or None,
                    }
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise TimeoutError(
                    f"IDMC task exceeded timeout of {self.timeout_seconds}s"
                )
            time.sleep(self.poll_interval_seconds)

    # ── StateBackedComponent contract ─────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        try:
            tasks = self._discover_tasks()
        except Exception:  # noqa: BLE001
            tasks = []
        snapshot = {
            "login_url": self.workspace.login_url,
            "tasks": tasks,
            "polled_at": time.time(),
        }
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        if state_path is None or not state_path.exists():
            return Definitions()

        state = json.loads(state_path.read_text())
        tasks = state.get("tasks", [])

        specs: List[AssetSpec] = []
        for t in tasks:
            props = InformaticaTaskProps(
                id=t["id"],
                name=t["name"],
                kind=t["kind"],
                folder_id=t.get("folder_id"),
                folder_path=t.get("folder_path"),
                description=t.get("description"),
            )
            specs.append(self.get_asset_spec(props))

        action = (self.action or "noop").lower()
        assets: List[Any] = []

        if action == "noop":
            assets = list(specs)
        elif action == "execute":
            _self = self

            @dg.multi_asset(specs=specs)
            def _informatica_execute(context: dg.AssetExecutionContext):
                session_id, base_url = _self._login()
                for spec in specs:
                    task = {
                        "id": spec.metadata["informatica/id"],
                        "name": spec.metadata["informatica/name"],
                        "kind": spec.metadata["informatica/kind"],
                    }
                    result = _self._execute_task(task, session_id, base_url, context)
                    if _self.wait_for_completion and result.get("run_status") not in IDMC_SUCCESS:
                        raise dg.Failure(
                            description=(
                                f"IDMC task {task['name']} finished with "
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

            assets = [_informatica_execute]
        else:
            raise ValueError(
                f"InformaticaWorkspaceComponent.action={action!r} not supported. "
                f"Use 'noop' or 'execute'."
            )

        sensors: List[Any] = []
        if self.polling_sensor and specs:
            sensors.append(self._build_observation_sensor(specs))

        checks: List[Any] = []
        if self.freshness_lag_threshold_seconds is not None and specs:
            checks.extend(self._build_freshness_checks(specs))

        return Definitions(assets=assets, sensors=sensors, asset_checks=checks)

    def _build_observation_sensor(self, specs: List[AssetSpec]):
        _self = self

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
                "/api/v2/activity/activityLog", sid, base, params={"top": 100}
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
                matching = [
                    s for s in specs
                    if s.metadata["informatica/name"] == task_name
                    and s.metadata["informatica/kind"] == task_kind
                ]
                for s in matching:
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

        return _observation_sensor

    def _build_freshness_checks(self, specs: List[AssetSpec]) -> List[Any]:
        from datetime import datetime, timezone
        _self = self
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
                    "/api/v2/activity/activityLog", sid, base, params={"top": 50}
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
        return checks
