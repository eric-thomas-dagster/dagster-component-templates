"""Vercel Deployment Sensor Component.

Polls the Vercel REST API for the terminal status of deployments for a
project (production/preview/development), emits an AssetMaterialization or
AssetObservation on ``READY``, and fires a Dagster job. Optionally fires on
terminal ``ERROR`` for downstream alerting flows.

Vercel deployment states (Vercel API v13):
  - READY         → success terminal (fire job + asset event)
  - ERROR         → failure terminal (opt-in fire via error_state_triggers_job)
  - CANCELED      → terminal, treated as skip
  - BUILDING      → in-progress
  - QUEUED        → in-progress
  - INITIALIZING  → in-progress

Auth: Vercel API token (Bearer). Create at
https://vercel.com/account/tokens. Team-scoped tokens can be used with
optional ``team_id`` query parameter.

API: GET /v13/deployments?projectId=<id>&target=<production|preview>&limit=10

Pair with ``external_vercel_deployment`` (recommended — Vercel owns the
build trigger via git-push) OR with ``vercel_deploy_trigger`` in
observation mode (Dagster fires the build via Deploy Hook then observes).

Docs: https://vercel.com/docs/rest-api/endpoints/deployments
"""
from typing import Dict, Optional

import dagster as dg
from dagster import (
    AssetMaterialization,
    AssetObservation,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field

VERCEL_TERMINAL_SUCCESS = {"READY"}
VERCEL_TERMINAL_FAIL = {"ERROR", "CANCELED"}


class VercelDeploymentSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire a Dagster job when a Vercel deployment reaches READY.

    Example — watch production deployments of a project:

        ```yaml
        type: dagster_community_components.VercelDeploymentSensorComponent
        attributes:
          sensor_name: vercel_prod_ready
          project_id: prj_AbCdEfGh12345
          target: production
          api_token_env_var: VERCEL_API_TOKEN
          job_name: post_deploy_smoke_tests_job
          asset_key: vercel/website/production
        ```

    Team-scoped:

        ```yaml
        attributes:
          # ...
          team_id: team_AbCdEfGh12345
          api_token_env_var: VERCEL_API_TOKEN
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    project_id: Optional[str] = Field(
        default=None,
        description="Vercel project ID (e.g. 'prj_...'). Set this OR project_name.",
    )
    project_name: Optional[str] = Field(
        default=None,
        description="Vercel project slug (the URL name). Set this OR project_id. `project_id` is preferred — it's stable across renames.",
    )
    target: str = Field(
        default="production",
        description="Deployment target: 'production' (default), 'preview', 'development', or 'any' (no filter).",
    )
    team_id: Optional[str] = Field(
        default=None,
        description="Vercel team ID (e.g. 'team_...'). Required for team-scoped projects.",
    )

    api_token_env_var: str = Field(
        default="VERCEL_API_TOKEN",
        description="Env var holding a Vercel API token (create at https://vercel.com/account/tokens).",
    )
    api_base_url: str = Field(
        default="https://api.vercel.com",
        description="Vercel API base URL (override for self-hosted proxies).",
    )

    job_name: str = Field(description="Dagster job to trigger on terminal success.")
    asset_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster asset key. When set, emits an AssetMaterialization / AssetObservation. '/'-separated.",
    )
    asset_event_type: str = Field(
        default="materialization",
        description="'materialization' (default) or 'observation'. Use 'observation' when paired with vercel_deploy_trigger.",
    )

    error_state_triggers_job: bool = Field(
        default=False,
        description="When true, ERROR deployments ALSO fire the job (with a tag) — useful for alert / rollback flows.",
    )

    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.project_id) == bool(self.project_name):
            raise ValueError(
                f"vercel_deployment_sensor {self.sensor_name!r}: set exactly one of "
                f"`project_id` or `project_name`."
            )
        if self.target not in {"production", "preview", "development", "any"}:
            raise ValueError(
                f"vercel_deployment_sensor {self.sensor_name!r}: target must be one of "
                f"production, preview, development, any."
            )

        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def vercel_deployment_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed (pip install requests)")

            token = os.environ.get(_self.api_token_env_var)
            if not token:
                return SensorResult(skip_reason=f"{_self.api_token_env_var} not set")

            params: Dict[str, str] = {"limit": "10"}
            if _self.project_id:
                params["projectId"] = _self.project_id
            else:
                # Fall back to the project slug lookup — Vercel accepts both.
                params["app"] = _self.project_name  # type: ignore[assignment]
            if _self.target and _self.target != "any":
                params["target"] = _self.target
            if _self.team_id:
                params["teamId"] = _self.team_id

            url = f"{_self.api_base_url.rstrip('/')}/v6/deployments"
            try:
                resp = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()
            except Exception as e:  # noqa: BLE001
                return SensorResult(skip_reason=f"Vercel API poll error: {e}")

            deployments = body.get("deployments") or []
            if not deployments:
                return SensorResult(skip_reason="No deployments found yet.")

            # Vercel returns newest-first by `created`. Find the newest with a
            # terminal state.
            terminal = [
                d for d in deployments
                if (d.get("state") or "").upper() in (VERCEL_TERMINAL_SUCCESS | VERCEL_TERMINAL_FAIL)
            ]
            if not terminal:
                return SensorResult(skip_reason="No terminal deployment yet.")
            latest = terminal[0]
            state = (latest.get("state") or "").upper()
            uid = latest.get("uid") or latest.get("id") or ""
            deploy_url = latest.get("url") or ""
            created = latest.get("createdAt") or latest.get("created") or ""
            meta = latest.get("meta") or {}

            cursor_key = uid
            if context.cursor == cursor_key:
                return SensorResult(skip_reason=f"Deployment {uid} already handled.")

            if state in VERCEL_TERMINAL_SUCCESS:
                return _build_result(
                    _self, context, latest, state, uid, deploy_url, created, meta, is_success=True,
                )

            if state in VERCEL_TERMINAL_FAIL and _self.error_state_triggers_job:
                return _build_result(
                    _self, context, latest, state, uid, deploy_url, created, meta, is_success=False,
                )

            if state in VERCEL_TERMINAL_FAIL:
                return SensorResult(
                    skip_reason=f"Deployment {uid} terminal failure: {state} — set error_state_triggers_job=true to fire the job on errors."
                )

            return SensorResult(skip_reason=f"Deployment {uid} state: {state}")

        return dg.Definitions(sensors=[vercel_deployment_sensor])


def _build_result(
    comp,
    context,
    deployment,
    state: str,
    uid: str,
    deploy_url: str,
    created,
    meta: Dict,
    is_success: bool,
):
    tags = {
        "vercel/deployment_uid": uid,
        "vercel/state": state,
        "vercel/target": comp.target,
    }
    if deploy_url:
        tags["vercel/deployment_url"] = deploy_url
    if meta.get("githubCommitSha"):
        tags["vercel/commit_sha"] = str(meta["githubCommitSha"])
    if meta.get("githubCommitRef"):
        tags["vercel/branch"] = str(meta["githubCommitRef"])

    asset_events = []
    if comp.asset_key:
        ak = dg.AssetKey(comp.asset_key.split("/"))
        md: Dict[str, object] = {
            "vercel_deployment_uid": uid,
            "vercel_state": state,
            "vercel_target": comp.target,
        }
        if deploy_url:
            md["vercel_deployment_url"] = dg.MetadataValue.url(f"https://{deploy_url}" if not str(deploy_url).startswith("http") else deploy_url)
        if created:
            md["vercel_created_at"] = str(created)
        if meta.get("githubCommitSha"):
            md["vercel_commit_sha"] = str(meta["githubCommitSha"])
        if meta.get("githubCommitRef"):
            md["vercel_branch"] = str(meta["githubCommitRef"])
        if meta.get("githubCommitMessage"):
            md["vercel_commit_message"] = str(meta["githubCommitMessage"])[:500]
        cls = AssetObservation if comp.asset_event_type == "observation" else AssetMaterialization
        asset_events.append(
            cls(
                asset_key=ak,
                description=f"Vercel deployment {uid} → {state}",
                metadata=md,
            )
        )

    return SensorResult(
        run_requests=[RunRequest(run_key=uid, tags=tags)],
        asset_events=asset_events or None,
        cursor=uid,
    )
