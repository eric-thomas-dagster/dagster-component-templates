"""TalendCloudWorkspaceComponent.

Wrap Talend Cloud (Talend Management Console API) behind a Dagster
workspace-shape component. Discovers every executable / job / plan in the
workspace and emits one AssetSpec per artifact. On materialize, POSTs
`/executions` and polls `/executions/{id}` until the artifact finishes.

Backing REST (Talend Cloud REST API v2.7):

    GET  {base}/workspaces/{workspace_id}/executables   — enumeration
    POST {base}/executions                              — trigger
        body: {"executable": "<id>", "workspaceId": "..."}
    GET  {base}/executions/{execution_id}               — poll status
    GET  {base}/executables/{id}                        — detail (optional)

Base URL by Talend Cloud region:
    US: https://api.us.cloud.talend.com/tmc/v2.7
    EU: https://api.eu.cloud.talend.com/tmc/v2.7
    AP: https://api.ap.cloud.talend.com/tmc/v2.7
    (custom "region: <full-url>" supported for private / other tenants)

Auth: `Authorization: Bearer <personal_access_token>`.

Execution status values (from GET /executions/{id}.status):
    PENDING       waiting to be picked up
    READY         picked up, engine assigned
    DEPLOYING     job being uploaded to engine
    RUNNING       actively executing
    TERMINATED    finished successfully
    CANCELED      user-canceled
    FAILED        error during run
    HOLD          held (rate limit, quota)
"""

from __future__ import annotations

import fnmatch
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


TALEND_TERMINAL: set = {"TERMINATED", "CANCELED", "FAILED"}
TALEND_SUCCESS: set = {"TERMINATED"}


_REGION_MAP = {
    "us": "https://api.us.cloud.talend.com/tmc/v2.7",
    "eu": "https://api.eu.cloud.talend.com/tmc/v2.7",
    "ap": "https://api.ap.cloud.talend.com/tmc/v2.7",
}


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
        description="Optional Talend Cloud environment ID (UUID). Restricts "
        "discovery + execution to this environment when set.",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="HTTP request timeout for individual API calls.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify TLS certs on requests. Set false only for lab tenants.",
    )


@dataclass
class TalendArtifact:
    """One row from GET /workspaces/{id}/executables."""
    id: str                    # UUID
    name: str
    kind: str                  # 'job' | 'plan' | 'route' | ...
    workspace_id: str
    environment_id: Optional[str]
    description: Optional[str]

    @property
    def qualified_name(self) -> str:
        return f"{self.kind}/{self.name}"


class TalendArtifactSelector(dg.Model):
    """Filter which Talend Cloud artifacts become Dagster assets."""

    by_kind: Optional[List[str]] = Field(
        default=None,
        description="Artifact kind restriction (`job` / `plan` / `route`). "
        "Default: all kinds.",
    )
    include: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns against `<kind>/<name>` (case-insensitive).",
    )
    exclude: Optional[List[str]] = Field(
        default=None, description="fnmatch patterns to EXCLUDE. Applied last."
    )


class TalendCloudWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Talend Cloud artifacts as Dagster assets.

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
        default=None,
        description="Filter which artifacts become assets. Default = all.",
    )
    action: str = Field(
        default="noop",
        description=(
            "materialize() behavior. `noop` = external asset (declare-only). "
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

    # ── Helpers ───────────────────────────────────────────────────
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

    def _discover_artifacts(self) -> List[TalendArtifact]:
        """Enumerate /workspaces/{id}/executables."""
        params = {"workspaceId": self.workspace.workspace_id}
        if self.workspace.environment_id:
            params["environmentId"] = self.workspace.environment_id
        data = self._http_get("/executables", params=params)
        # Talend Cloud returns a list of executable objects (paginated response
        # in some tenants — this MVP handles the flat-list shape).
        artifacts = data if isinstance(data, list) else data.get("items", []) or []

        result: List[TalendArtifact] = []
        for a in artifacts:
            result.append(
                TalendArtifact(
                    id=str(a.get("id") or a.get("executableId") or ""),
                    name=str(a.get("name") or a.get("artifactName") or ""),
                    kind=(a.get("type") or a.get("artifactType") or "job").lower(),
                    workspace_id=self.workspace.workspace_id,
                    environment_id=self.workspace.environment_id,
                    description=a.get("description"),
                )
            )

        sel = self.artifact_selector
        if not sel:
            return result

        def _match(a: TalendArtifact) -> bool:
            if sel.by_kind and a.kind not in [k.lower() for k in sel.by_kind]:
                return False
            qname = a.qualified_name.lower()
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

    def _execute_artifact(self, artifact: TalendArtifact, context) -> Dict[str, Any]:
        """POST /executions + poll /executions/{id} to completion."""
        body: Dict[str, Any] = {
            "executable": artifact.id,
            "workspaceId": self.workspace.workspace_id,
        }
        if self.workspace.environment_id:
            body["environmentId"] = self.workspace.environment_id

        r = self._http_post("/executions", body)
        execution_id = str(r.get("id") or r.get("executionId") or "")
        if not execution_id:
            raise RuntimeError(
                f"POST /executions for {artifact.qualified_name} returned no execution id: {r}"
            )
        context.log.info(
            f"Talend artifact {artifact.qualified_name} triggered — execution_id={execution_id}"
        )

        if not self.wait_for_completion:
            return {
                "execution_id": execution_id,
                "status": None,
                "error_message": None,
            }

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
                        or "Talend execution finished non-successfully; see execution details in TMC."
                    )
                return {
                    "execution_id": execution_id,
                    "status": status,
                    "error_message": error_message,
                    "duration_seconds": info.get("durationInMillis", 0) / 1000
                    if info.get("durationInMillis")
                    else None,
                }
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise TimeoutError(
                    f"Talend artifact {artifact.qualified_name} exceeded timeout of "
                    f"{self.timeout_seconds}s (last status={status!r})"
                )
            time.sleep(self.poll_interval_seconds)

    # ── build_defs ────────────────────────────────────────────────
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        try:
            artifacts = self._discover_artifacts()
        except Exception as e:
            import warnings
            warnings.warn(
                f"TalendCloudWorkspaceComponent: could not discover artifacts "
                f"({type(e).__name__}: {e}). Emitting empty definitions."
            )
            return dg.Definitions()

        prefix = self.asset_key_prefix or [
            "talend_cloud",
            self.workspace.workspace_id[:8] if self.workspace.workspace_id else "workspace",
        ]
        kinds = self.kinds or ["talend"]

        specs: List[dg.AssetSpec] = []
        for a in artifacts:
            key_path = [*prefix, a.kind, a.name]
            spec = dg.AssetSpec(
                key=dg.AssetKey(key_path),
                description=(
                    a.description
                    or f"Talend Cloud {a.kind} `{a.name}` (id={a.id})"
                ),
                group_name=self.group_name,
                kinds=set(kinds),
                tags=self.tags or {},
                owners=self.owners or [],
                metadata={
                    "talend/id": a.id,
                    "talend/name": a.name,
                    "talend/kind": a.kind,
                    "talend/workspace_id": a.workspace_id,
                    **(
                        {"talend/environment_id": a.environment_id}
                        if a.environment_id
                        else {}
                    ),
                },
            )
            specs.append(spec)

        action = (self.action or "noop").lower()

        if action == "noop":
            defs_kwargs: Dict[str, Any] = {"assets": specs}
        elif action == "execute":
            @dg.multi_asset(specs=specs)
            def _talend_execute(context: dg.AssetExecutionContext):
                for spec in specs:
                    artifact = TalendArtifact(
                        id=spec.metadata["talend/id"],
                        name=spec.metadata["talend/name"],
                        kind=spec.metadata["talend/kind"],
                        workspace_id=spec.metadata["talend/workspace_id"],
                        environment_id=spec.metadata.get("talend/environment_id"),
                        description=None,
                    )
                    result = _self._execute_artifact(artifact, context)
                    if _self.wait_for_completion and result.get("status") not in TALEND_SUCCESS:
                        raise dg.Failure(
                            description=(
                                f"Talend {artifact.qualified_name} finished with "
                                f"status={result.get('status')!r}: "
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

            defs_kwargs = {"assets": [_talend_execute]}
        else:
            raise ValueError(
                f"TalendCloudWorkspaceComponent.action={action!r} not supported. "
                f"Use 'noop' or 'execute'."
            )

        if self.polling_sensor and specs:
            @dg.sensor(
                name="talend_cloud_workspace_observation_sensor",
                minimum_interval_seconds=self.observation_interval_seconds,
                default_status=dg.DefaultSensorStatus.STOPPED,
                asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
            )
            def _observation_sensor(context: dg.SensorEvaluationContext):
                # Cursor = high-water execution timestamp (ISO 8601).
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

            defs_kwargs["sensors"] = [_observation_sensor]

        if self.freshness_lag_threshold_seconds is not None and specs:
            from datetime import datetime, timezone
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

            defs_kwargs["asset_checks"] = checks

        return dg.Definitions(**defs_kwargs)
