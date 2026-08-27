"""TalendCloudWorkspaceComponent.

Wrap Talend Cloud (Talend Management Console API) behind a Dagster
workspace-shape component. Discovers every executable / job / plan in the
workspace and emits one AssetSpec per artifact. On materialize, POSTs
`/executions` and polls `/executions/{id}` until the artifact finishes.

Full workspace-pattern shape (parity with hvr_hub_workspace / snowflake_workspace):
  - `@public` class annotation
  - `@record` props class
  - `translation:` callable for per-asset customization
  - `StateBackedComponent` inheritance — discovery cached to disk via
    `write_state_to_path`. Refresh via `dg utils refresh-defs-state`.

Backing REST (Talend Cloud REST API v2.7):

    GET  {base}/workspaces/{workspace_id}/executables   — enumeration
    POST {base}/executions                              — trigger
    GET  {base}/executions/{execution_id}               — poll status

Base URL by Talend Cloud region:
    US: https://api.us.cloud.talend.com/tmc/v2.7
    EU: https://api.eu.cloud.talend.com/tmc/v2.7
    AP: https://api.ap.cloud.talend.com/tmc/v2.7
    (custom "region: <full-url>" supported for private / other tenants)

Auth: `Authorization: Bearer <personal_access_token>`.
"""

import fnmatch
import hashlib
import json
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

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


TALEND_TERMINAL: set = {"TERMINATED", "CANCELED", "FAILED"}
TALEND_SUCCESS: set = {"TERMINATED"}


_REGION_MAP = {
    "us": "https://api.us.cloud.talend.com/tmc/v2.7",
    "eu": "https://api.eu.cloud.talend.com/tmc/v2.7",
    "ap": "https://api.ap.cloud.talend.com/tmc/v2.7",
}


# ── Props (@record) for translator callable ─────────────────────────
@record
class TalendArtifactProps:
    """Data passed to `translation:` callables for each Talend artifact.

    Attributes:
        id: Global artifact ID (UUID).
        name: Artifact name.
        kind: `job` | `plan` | `route` etc.
        workspace_id: Talend Cloud workspace UUID.
        environment_id: Optional environment UUID.
        description: The artifact's own description string.
    """

    id: str
    name: str
    kind: str
    workspace_id: str
    environment_id: Optional[str] = None
    description: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        return f"{self.kind}/{self.name}"


# ── Workspace config nested block ───────────────────────────────────
class TalendCloudWorkspaceConfig(dg.Model):
    """Talend Cloud (TMC) connection."""

    region: str = Field(
        default="us",
        description=(
            "Talend Cloud region key (`us` / `eu` / `ap`) OR a full base URL "
            "for private / other tenants (must start with `http`)."
        ),
    )
    workspace_id: str = Field(
        description="Talend Cloud workspace ID (UUID). From TMC UI or /workspaces API."
    )
    auth_token_env_var: str = Field(
        description="Env var containing the Talend Cloud personal access token "
        "(or service account token). Sent as `Authorization: Bearer <token>`."
    )
    environment_id: Optional[str] = Field(
        default=None,
        description="Optional Talend Cloud environment ID (UUID).",
    )
    request_timeout_seconds: int = Field(default=60)
    verify_ssl: bool = Field(default=True)


# ── Selector block ──────────────────────────────────────────────────
class TalendArtifactSelector(dg.Model):
    """Filter which Talend Cloud artifacts become Dagster assets."""

    by_kind: Optional[List[str]] = Field(
        default=None,
        description="Artifact kind restriction (`job` / `plan` / `route`). Default: all.",
    )
    include: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns against `<kind>/<name>` (case-insensitive).",
    )
    exclude: Optional[List[str]] = Field(
        default=None, description="fnmatch patterns to EXCLUDE. Applied last."
    )


# ── Base translator ─────────────────────────────────────────────────
class TalendCloudComponentTranslator:
    """Base translator: TalendArtifactProps → AssetSpec."""

    def __init__(self, component: "TalendCloudWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: TalendArtifactProps) -> AssetSpec:
        prefix = self._component.asset_key_prefix or [
            "talend_cloud",
            props.workspace_id[:8] if props.workspace_id else "workspace",
        ]
        return AssetSpec(
            key=AssetKey([*prefix, props.kind, props.name]),
            description=(
                props.description
                or f"Talend Cloud {props.kind} `{props.name}` (id={props.id})"
            ),
            group_name=self._component.group_name,
            # Dagster kinds — Talend has no first-class icon in Dagster,
            # so these render as text-only badges. Still useful for
            # catalog filtering (`kind:etl`, `kind:talend`).
            kinds=set(self._component.kinds or ["talend", "etl"]),
            tags=dict(self._component.tags or {}),
            owners=list(self._component.owners or []),
            metadata={
                "talend/id": props.id,
                "talend/name": props.name,
                "talend/kind": props.kind,
                "talend/workspace_id": props.workspace_id,
                **({"talend/environment_id": props.environment_id} if props.environment_id else {}),
            },
        )


# ── Component ───────────────────────────────────────────────────────
@public
class TalendCloudWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Talend Cloud artifacts as Dagster assets — full workspace-pattern shape.

    Example:

    ```yaml
    type: dagster_community_components.TalendCloudWorkspaceComponent
    attributes:
      workspace:
        region:             us
        workspace_id:       "{{ env.TALEND_WORKSPACE_ID }}"
        auth_token_env_var: TALEND_API_TOKEN
      artifact_selector:
        by_kind: [job]
        include: ["etl_*"]
        exclude: ["*_test"]
      # translation: |
      #   {{ load_python_module_attr('my_project.talend.translate.by_environment') }}
      action: execute
      wait_for_completion: true
      poll_interval_seconds: 30
      timeout_seconds: 3600
      polling_sensor: true
      observation_interval_seconds: 300
      freshness_lag_threshold_seconds: 3600
      group_name: talend_prod
      kinds: [talend, etl]
    ```
    """

    workspace: TalendCloudWorkspaceConfig = Field(
        description="Talend Cloud connection details."
    )
    artifact_selector: Optional[TalendArtifactSelector] = Field(
        default=None, description="Filter which artifacts become assets. Default = all."
    )
    translation: Annotated[
        Optional[TranslationFn[TalendArtifactProps]],
        TranslationFnResolver(
            template_vars_for_translation_fn=lambda data: {"props": data}
        ),
    ] = Field(
        default=None,
        description=(
            "Optional per-asset translation callable. Receives a "
            "TalendArtifactProps and returns AssetSpec overrides. Use for "
            "per-environment / per-kind customization beyond uniform group/tags."
        ),
    )
    action: str = Field(
        default="noop",
        description=(
            "materialize() behavior. `noop` = external asset. "
            "`execute` = POST /executions + poll."
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
        description="Default: `['talend_cloud', <workspace_id_short>]`.",
    )
    group_name: Optional[str] = Field(default="talend_cloud")
    kinds: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description="StateBackedComponent state config. Default: local filesystem cache.",
    )

    # ── Base translator ─────────────────────────────────────────────
    @property
    def _base_translator(self) -> TalendCloudComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = TalendCloudComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @public
    def get_asset_spec(self, props: TalendArtifactProps) -> AssetSpec:
        base_spec = self._base_translator.get_asset_spec(props)
        if self.translation is None:
            return base_spec
        overrides = self.translation(base_spec, props) or {}
        if isinstance(overrides, AssetSpec):
            return overrides
        return base_spec._replace(**overrides) if hasattr(base_spec, "_replace") else base_spec

    @property
    def defs_state_config(self) -> DefsStateConfig:
        composite = f"{self.workspace.region}::{self.workspace.workspace_id}"
        state_hash = hashlib.sha256(composite.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{state_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    # ── Runtime helpers ────────────────────────────────────────────
    def _base_url(self) -> str:
        r = (self.workspace.region or "us").lower().strip()
        if r.startswith("http"):
            return r.rstrip("/")
        if r not in _REGION_MAP:
            raise ValueError(
                f"Talend region {r!r} not recognized. Use one of {list(_REGION_MAP.keys())} "
                f"or supply a full URL starting with http."
            )
        return _REGION_MAP[r]

    def _auth_header(self) -> Dict[str, str]:
        import os
        token = os.environ.get(self.workspace.auth_token_env_var, "")
        if not token:
            raise ValueError(
                f"env var {self.workspace.auth_token_env_var!r} is empty or unset"
            )
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def _http_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        import requests
        r = requests.get(
            f"{self._base_url()}{path}",
            headers=self._auth_header(),
            params=params or {},
            timeout=self.workspace.request_timeout_seconds,
            verify=self.workspace.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def _http_post(self, path: str, json_body: Dict[str, Any]) -> Any:
        import requests
        r = requests.post(
            f"{self._base_url()}{path}",
            headers={**self._auth_header(), "Content-Type": "application/json"},
            json=json_body,
            timeout=self.workspace.request_timeout_seconds,
            verify=self.workspace.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def _discover_artifacts(self) -> List[Dict[str, Any]]:
        params = {"workspaceId": self.workspace.workspace_id}
        if self.workspace.environment_id:
            params["environmentId"] = self.workspace.environment_id
        data = self._http_get("/executables", params=params)
        artifacts = data if isinstance(data, list) else data.get("items", []) or []

        result: List[Dict[str, Any]] = []
        for a in artifacts:
            result.append({
                "id": str(a.get("id") or a.get("executableId") or ""),
                "name": str(a.get("name") or a.get("artifactName") or ""),
                "kind": (a.get("type") or a.get("artifactType") or "job").lower(),
                "workspace_id": self.workspace.workspace_id,
                "environment_id": self.workspace.environment_id,
                "description": a.get("description"),
            })

        sel = self.artifact_selector
        if not sel:
            return result

        def _match(a: Dict[str, Any]) -> bool:
            if sel.by_kind and a["kind"] not in [k.lower() for k in sel.by_kind]:
                return False
            qname = f"{a['kind']}/{a['name']}".lower()
            if sel.include and not any(
                fnmatch.fnmatch(qname, pat.lower()) for pat in sel.include
            ):
                return False
            if sel.exclude and any(
                fnmatch.fnmatch(qname, pat.lower()) for pat in sel.exclude
            ):
                return False
            return True

        return [a for a in result if _match(a)]

    def _execute_artifact(self, artifact: Dict[str, Any], context) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "executable": artifact["id"],
            "workspaceId": self.workspace.workspace_id,
        }
        if self.workspace.environment_id:
            body["environmentId"] = self.workspace.environment_id

        r = self._http_post("/executions", body)
        execution_id = str(r.get("id") or r.get("executionId") or "")
        if not execution_id:
            raise RuntimeError(
                f"POST /executions for {artifact.get('kind')}/{artifact.get('name')} "
                f"returned no execution id: {r}"
            )
        context.log.info(
            f"Talend artifact {artifact.get('kind')}/{artifact.get('name')} triggered "
            f"— execution_id={execution_id}"
        )

        if not self.wait_for_completion:
            return {"execution_id": execution_id, "status": None, "error_message": None}

        start = time.time()
        while True:
            info = self._http_get(f"/executions/{execution_id}")
            status = str(info.get("status") or "").upper()
            if status in TALEND_TERMINAL:
                error_message = None
                if status not in TALEND_SUCCESS:
                    error_message = (
                        info.get("errorMessage")
                        or info.get("failureType")
                        or "Talend execution finished non-successfully; see TMC."
                    )
                return {
                    "execution_id": execution_id,
                    "status": status,
                    "error_message": error_message,
                }
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise TimeoutError(
                    f"Talend artifact exceeded timeout of {self.timeout_seconds}s "
                    f"(last status={status!r})"
                )
            time.sleep(self.poll_interval_seconds)

    # ── StateBackedComponent contract ─────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        try:
            artifacts = self._discover_artifacts()
        except Exception:  # noqa: BLE001
            artifacts = []
        snapshot = {
            "workspace_id": self.workspace.workspace_id,
            "artifacts": artifacts,
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
        artifacts = state.get("artifacts", [])

        specs: List[AssetSpec] = []
        for a in artifacts:
            props = TalendArtifactProps(
                id=a["id"],
                name=a["name"],
                kind=a["kind"],
                workspace_id=a["workspace_id"],
                environment_id=a.get("environment_id"),
                description=a.get("description"),
            )
            specs.append(self.get_asset_spec(props))

        action = (self.action or "noop").lower()
        assets: List[Any] = []

        if action == "noop":
            assets = list(specs)
        elif action == "execute":
            _self = self

            @dg.multi_asset(specs=specs)
            def _talend_execute(context: dg.AssetExecutionContext):
                for spec in specs:
                    artifact = {
                        "id": spec.metadata["talend/id"],
                        "name": spec.metadata["talend/name"],
                        "kind": spec.metadata["talend/kind"],
                    }
                    result = _self._execute_artifact(artifact, context)
                    if _self.wait_for_completion and result.get("status") not in TALEND_SUCCESS:
                        raise dg.Failure(
                            description=(
                                f"Talend {artifact['kind']}/{artifact['name']} "
                                f"finished with status={result.get('status')!r}: "
                                f"{result.get('error_message')}"
                            )
                        )
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "talend/execution_id": result["execution_id"],
                            "talend/status": result.get("status") or "async",
                        },
                    )

            assets = [_talend_execute]
        else:
            raise ValueError(
                f"TalendCloudWorkspaceComponent.action={action!r} not supported. "
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
        prefix = self.asset_key_prefix or [
            "talend_cloud",
            self.workspace.workspace_id[:8] if self.workspace.workspace_id else "workspace",
        ]

        @dg.sensor(
            name="talend_cloud_workspace_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            cursor_val = context.cursor or ""
            params: Dict[str, Any] = {
                "workspaceId": _self.workspace.workspace_id,
                "limit": 100,
                "orderBy": "-startTimestamp",
            }
            data = _self._http_get("/executions", params=params)
            executions = data if isinstance(data, list) else data.get("items", [])

            observations = []
            new_cursor = cursor_val
            for ex in executions:
                ts = ex.get("startTimestamp") or ""
                if cursor_val and ts <= cursor_val:
                    continue
                if ex.get("status", "").upper() not in TALEND_TERMINAL:
                    continue
                new_cursor = max(new_cursor, ts) if new_cursor else ts
                ex_id = ex.get("executable", {}) or {}
                ex_kind = (ex_id.get("type") or ex.get("executableType") or "job").lower()
                ex_name = ex_id.get("name") or ex.get("executableName") or ""
                if not ex_name:
                    continue
                key_path = [*prefix, ex_kind, ex_name]
                observations.append(
                    dg.AssetObservation(
                        asset_key=dg.AssetKey(key_path),
                        metadata={
                            "talend/execution_id": ex.get("id") or "",
                            "talend/status": ex.get("status") or "",
                            "talend/start": ex.get("startTimestamp") or "",
                            "talend/finish": ex.get("finishTimestamp") or "",
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
                name="talend_freshness_lag",
                description=(
                    f"Fails when the last successful Talend execution of "
                    f"this artifact is older than {threshold}s."
                ),
            )
            def _check(_key=spec.key):
                artifact_id = next(
                    s.metadata["talend/id"] for s in specs if s.key == _key
                )
                params = {
                    "workspaceId": _self.workspace.workspace_id,
                    "executable": artifact_id,
                    "status": "TERMINATED",
                    "limit": 1,
                    "orderBy": "-finishTimestamp",
                }
                data = _self._http_get("/executions", params=params)
                items = data if isinstance(data, list) else data.get("items", [])
                if not items:
                    return dg.AssetCheckResult(
                        passed=False, description="No successful executions found."
                    )
                finish = items[0].get("finishTimestamp")
                if not finish:
                    return dg.AssetCheckResult(
                        passed=False,
                        description="Most recent execution has no finishTimestamp.",
                    )
                end_time = datetime.fromisoformat(finish.replace("Z", "+00:00"))
                lag = (datetime.now(timezone.utc) - end_time).total_seconds()
                return dg.AssetCheckResult(
                    passed=lag <= threshold,
                    description=f"lag={int(lag)}s (threshold={threshold}s)",
                    metadata={
                        "talend/last_success_at": str(end_time),
                        "talend/lag_seconds": int(lag),
                    },
                )
            checks.append(_check)
        return checks
