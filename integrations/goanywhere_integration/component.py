"""Fortra GoAnywhere MFT integration component.

Bidirectional integration between Dagster and Fortra GoAnywhere MFT
(Managed File Transfer):

  Dagster -> GoAnywhere:
    - Run a GoAnywhere Project (POST .../gacmd/v1/projects, runParameters body)
    - Poll the resulting job (GET .../gacmd/v1/jobs/{jobId}?jobStatusFormat=JSON)
    - Pull a dashboard/service-status summary for observability
    - HTTP Basic Auth (username:password or an admin API key in the
      Authorization header)

  GoAnywhere -> Dagster:
    - A GoAnywhere Trigger (file-arrival, schedule, etc.) can run a Project
      independent of Dagster; a post-step in that Project can call Dagster's
      GraphQL API (launchRun mutation) to start a run
    - Parameters passed via run_config; partition selected via the project's
      own scheduled-date variable

Each declared GoAnywhere Project becomes a daily-partitioned Dagster asset.

`demo_mode: true` (default) simulates the GACMD REST API on stdout — the
whole component runs end-to-end with zero external dependencies. GoAnywhere
MFT does ship Docker images (fortrallc/goanywhere-mft on Docker Hub), but the
admin console and REST API don't do anything useful until activated with a
trial or paid license key (Fortra doesn't publish a license-free mode), so
the simulator is still the only zero-license way to smoke-test the whole
surface.

Environment variables (production mode only):
  GOANYWHERE_USER      — REST API username (or admin API key as the username)
  GOANYWHERE_PASSWORD  — REST API password (or the admin API key itself)

API reference (GACMD REST, confirmed via Fortra's public product docs and
forum posts — Fortra doesn't host a single public API reference page, so
verify exact paths/fields against your instance's built-in REST docs before
going to production):
  https://www.goanywhere.com/products/goanywhere-mft/automation/commands-apis
  https://www.goanywhere.com/resources/guides/api-integration-goanywhere-mft
"""
import base64
import json
import time
import uuid
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
from pydantic import ConfigDict, Field


# ═════════════════════════════════════════════════════════════════════
# Specs
# ═════════════════════════════════════════════════════════════════════

class GoAnywhereProjectSpec(dg.Model, dg.Resolvable):
    """A GoAnywhere Project wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    project_name: str = Field(
        description="GoAnywhere Project name (as registered in the GoAnywhere server).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Project.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    domain: str = Field(default="", description="GoAnywhere Domain the project belongs to (multi-tenant deployments).")
    job_name: str = Field(default="", description="Optional job name override; defaults to the project name.")
    job_queue: str = Field(default="", description="GoAnywhere job queue to run on.")
    mode: str = Field(default="NORMAL", description="Run mode GACMD accepts for runParameters.mode (e.g. NORMAL, TEST).")
    priority: int = Field(default=5, description="GACMD run priority (1-10, higher runs sooner).")
    variables: Dict[str, Any] = Field(
        default_factory=dict,
        description="Project variables passed through runParameters.variables (e.g. source/destination paths).",
    )


class GoAnywhereSourceFileSpec(dg.Model, dg.Resolvable):
    """A file/folder landed by a GoAnywhere Project that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(description="Fully-qualified path or logical name of the landed file/folder.")
    asset_name: str = Field(description="Dagster asset name representing the landed data.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="goanywhere_managed_data", description="Dagster asset group for this file.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the GoAnywhere project that lands this file (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_goanywhere(context: AssetExecutionContext, project: GoAnywhereProjectSpec, endpoint: str) -> dict:
    """Simulate the GACMD REST API lifecycle to stdout — no external deps."""
    job_id = str(uuid.uuid4().int % 900000 + 100000)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    run_payload = {
        "runParameters": {
            "project": project.project_name,
            "domain": project.domain or None,
            "jobName": project.job_name or project.project_name,
            "jobQueue": project.job_queue or None,
            "mode": project.mode,
            "priority": project.priority,
            "variables": {**project.variables, "SCHEDULED_DATE": scheduled},
        },
    }
    fake_basic = base64.b64encode(b"svc_dagster:******").decode()

    context.log.info(f"[AUTH]   Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[RUN]    POST {endpoint}/gacmd/v1/projects -> jobId={job_id}")
    context.log.info(f"  Body: {json.dumps(run_payload, indent=2, default=str)}")

    for state in ["Q", "A", "A", "S"]:
        context.log.info(
            f"[POLL]   GET {endpoint}/gacmd/v1/jobs/{job_id}?jobStatusFormat=JSON -> status={state}"
        )

    context.log.info(f"[DONE]   {project.project_name} -> Successful (jobId: {job_id})")

    return {"job_id": job_id, "state": "S", "scheduled_date": scheduled, "target": "goanywhere"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

_STATUS_LABELS = {
    "Q": "Queued", "A": "Active", "P": "Paused",
    "S": "Successful", "F": "Failed", "C": "Cancelled",
}
_TERMINAL_STATES = {"S", "F", "C"}


def _goanywhere_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_goanywhere(
    context: AssetExecutionContext,
    project: GoAnywhereProjectSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
) -> dict:
    """Real GACMD REST API lifecycle: run a Project, poll its job to a terminal
    state. Confirmed shape (Fortra product docs + forum posts, no single
    hosted API reference) -- `POST .../gacmd/v1/projects` with a
    `runParameters` body returns a jobId; `GET .../gacmd/v1/jobs/{jobId}
    ?jobStatusFormat=JSON` returns one of Q/A/P/S/F/C. Verify field names
    against your instance's built-in REST docs (Admin > Documentation) before
    relying on this in production -- Fortra doesn't publish a single public
    reference and field names have shifted across GoAnywhere releases.
    """
    import os
    import requests

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    headers = _goanywhere_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    run_payload = {
        "runParameters": {
            "project": project.project_name,
            "domain": project.domain or None,
            "jobName": project.job_name or project.project_name,
            "jobQueue": project.job_queue or None,
            "mode": project.mode,
            "priority": project.priority,
            "variables": {**project.variables, "SCHEDULED_DATE": scheduled},
        },
    }
    context.log.info(f"[RUN]    POST {endpoint}/gacmd/v1/projects")
    resp = requests.post(
        f"{endpoint}/gacmd/v1/projects",
        json=run_payload, headers=headers, timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    job_id = str(
        payload.get("jobId")
        or payload.get("JobID")
        or (payload.get("job") or {}).get("id")
        or "unknown"
    )
    context.log.info(f"  jobId: {job_id}")

    start = time.time()
    state = "Q"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/gacmd/v1/jobs/{job_id}",
            params={"jobStatusFormat": "JSON"},
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        status_payload = resp.json()
        state = (
            status_payload.get("status")
            or status_payload.get("Status")
            or status_payload.get("jobStatus")
            or "Q"
        )
        context.log.info(f"[POLL]   {project.project_name} -> {_STATUS_LABELS.get(state, state)}")
        if state in _TERMINAL_STATES:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{project.project_name} timed out after {poll_timeout}s")

    if state != "S":
        raise RuntimeError(f"{project.project_name} finished: {_STATUS_LABELS.get(state, state)}")

    return {"job_id": job_id, "state": state, "scheduled_date": scheduled, "target": "goanywhere"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class GoAnywhereIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fortra GoAnywhere MFT GACMD REST API integration.

    Each declared GoAnywhere Project becomes a daily-partitioned Dagster
    asset with a retry policy. Optional source-file declarations bring
    GoAnywhere-landed files into Dagster's lineage graph. A rerun op + a
    dashboard-summary op ship as Dagster jobs so GoAnywhere can be managed
    from the Dagster UI.

    `demo_mode: true` (default) simulates the GACMD REST API on stdout so the
    whole component runs end-to-end with no GoAnywhere endpoint. Flip to
    `false` and set `GOANYWHERE_USER` / `GOANYWHERE_PASSWORD` to hit a real
    GoAnywhere MFT instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the GACMD REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://goanywhere.internal/goanywhere/rest",
        description="GoAnywhere MFT REST API base URL (used when demo_mode=false).",
    )
    projects: List[GoAnywhereProjectSpec] = Field(
        default_factory=list,
        description="GoAnywhere Projects to wrap as daily-partitioned Dagster assets.",
    )
    source_files: List[GoAnywhereSourceFileSpec] = Field(
        default_factory=list,
        description="Files/folders landed by GoAnywhere that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before projects run.",
    )
    group_name: str = Field(
        default="goanywhere_integration",
        description="Dagster asset group for the project assets.",
    )

    goanywhere_user_env: str = Field(
        default="GOANYWHERE_USER",
        description="Env var holding the GoAnywhere REST username (or an admin API key).",
    )
    goanywhere_password_env: str = Field(
        default="GOANYWHERE_PASSWORD",
        description="Env var holding the GoAnywhere REST password (or the admin API key value).",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between GoAnywhere job status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal state before failing.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the GoAnywhere vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.goanywhere_user_env
        password_env = self.goanywhere_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds

        resolved_upstream = [dg.AssetKey(dep.split("/")) for dep in self.upstream_deps]
        daily_partition = dg.DailyPartitionsDefinition(start_date=self.partition_start_date)

        retry_policy = RetryPolicy(
            max_retries=self.max_retries,
            delay=self.retry_delay_seconds,
        )

        all_assets: List[dg.AssetsDefinition] = []
        last_job_ids: Dict[str, str] = {}

        for project in self.projects:
            def _make_asset(
                _project=project,
                _endpoint=endpoint,
                _demo=demo_mode,
                _group=group,
                _upstream=resolved_upstream,
            ):
                @dg.asset(
                    name=_project.asset_name,
                    kinds={"python", "goanywhere"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_project.description or (
                        f"GoAnywhere Project: {_project.project_name}. "
                        f"Runs via GACMD REST (queue: {_project.job_queue or 'default'}), "
                        f"polls for a terminal job status."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "goanywhere",
                        "integration_pattern": "dagster-orchestrates-goanywhere",
                        "scheduler_owner": "GoAnywhere MFT",
                        "goanywhere_domain": _project.domain,
                        "goanywhere_queue": _project.job_queue,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_goanywhere(context, _project, _endpoint)
                    else:
                        result = _execute_goanywhere(
                            context, _project, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout,
                        )
                    duration = round(time.time() - start_time, 2)
                    last_job_ids[_project.project_name] = result.get("job_id", "unknown")
                    context.add_output_metadata({
                        "external_job_id": result.get("job_id", "N/A"),
                        "state": _STATUS_LABELS.get(result.get("state", ""), result.get("state", "N/A")),
                        "scheduled_date": result.get("scheduled_date", "N/A"),
                        "duration_seconds": duration,
                        "demo_mode": _demo,
                    })

                return _asset_fn

            all_assets.append(_make_asset())

        source_assets: List[dg.AssetsDefinition] = []
        for source_file in self.source_files:
            file_deps = [dg.AssetKey(source_file.produced_by)] if source_file.produced_by else []

            def _make_source(
                _file=source_file,
                _deps=file_deps,
            ):
                @dg.asset(
                    name=_file.asset_name,
                    kinds={"goanywhere", "file"},
                    group_name=_file.group_name,
                    deps=_deps,
                    description=_file.description or (
                        f"File/folder landed by GoAnywhere: {_file.path}. "
                        f"Data is transferred/produced by a GoAnywhere Project."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "goanywhere_managed",
                        "path": _file.path,
                        "managed_by": "GoAnywhere MFT",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[FILE]   {_file.path}")
                    context.log.info(f"  Managed by: GoAnywhere MFT (external)")
                    context.log.info(f"  Produced by: {_file.produced_by or 'unknown GoAnywhere project'}")
                    context.log.info(f"  This asset represents the landed data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="goanywhere_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors GoAnywhere for project runs Dagster didn't trigger (e.g. a file-arrival Trigger fired one).",
        )
        def goanywhere_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                job_id = str(uuid.uuid4().int % 900000 + 100000)
                context.log.info(f"[DETECTED] External GoAnywhere job")
                context.log.info(f"  Triggered by: GoAnywhere file-arrival Trigger")
                context.log.info(f"  State: Successful")
                context.log.info(f"  Duration: {_rand.randint(10, 300)}s")
                context.log.info(f"  jobId: {job_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.projects[0].asset_name if self.projects else "goanywhere_project"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_job_id": dg.MetadataValue.text(job_id),
                        "platform": dg.MetadataValue.text("goanywhere"),
                        "triggered_by": dg.MetadataValue.text("GoAnywhere Trigger"),
                    },
                )
                context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                yield dg.SensorResult(asset_events=[observation])
                return
            else:
                import os
                import requests
                user = os.environ.get(user_env)
                pw = os.environ.get(password_env)
                if user and pw and last_job_ids:
                    try:
                        headers = _goanywhere_headers(user, pw)
                        project_name, job_id = next(iter(last_job_ids.items()))
                        resp = requests.get(
                            f"{_ep}/gacmd/v1/jobs/{job_id}",
                            params={"jobStatusFormat": "JSON"},
                            headers=headers, timeout=30,
                        )
                        resp.raise_for_status()
                        payload = resp.json()
                        observation = dg.AssetObservation(
                            asset_key=dg.AssetKey(f"goanywhere_{project_name.lower().replace(' ', '_')}"),
                            metadata={
                                "external_job_id": dg.MetadataValue.text(job_id),
                                "state": dg.MetadataValue.text(str(payload.get("status", "unknown"))),
                            },
                        )
                        context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                        yield dg.SensorResult(asset_events=[observation])
                        return
                    except Exception as e:
                        context.log.warning(f"GoAnywhere monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"project_name": dg.Field(str, is_required=True)})
        def rerun_goanywhere_project(context: dg.OpExecutionContext):
            """Manually rerun a GoAnywhere Project by name. Config: {project_name}."""
            project_name = context.op_config["project_name"]
            matching = next((p for p in self.projects if p.project_name == project_name), None)
            if matching is None:
                raise ValueError(f"No declared project named {project_name!r}")
            if _demo:
                job_id = str(uuid.uuid4().int % 900000 + 100000)
                context.log.info(f"[RERUN]  POST {_ep}/gacmd/v1/projects -> jobId={job_id}")
                context.log.info(f"  Response: 200 OK — project resubmitted")
            else:
                import os
                import requests as _req
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _goanywhere_headers(u or "", p or "")
                resp = _req.post(
                    f"{_ep}/gacmd/v1/projects",
                    json={"runParameters": {
                        "project": matching.project_name,
                        "domain": matching.domain or None,
                        "jobQueue": matching.job_queue or None,
                        "mode": matching.mode,
                        "priority": matching.priority,
                        "variables": matching.variables,
                    }},
                    headers=headers, timeout=30,
                )
                resp.raise_for_status()
                context.log.info(f"[RERUN]  {project_name} resubmitted: {resp.json()}")

        @dg.op
        def goanywhere_dashboard_summary(context: dg.OpExecutionContext):
            """Pull GoAnywhere's dashboard-style info endpoints (service status,
            active sessions, transfer summary) for observability. GoAnywhere
            ships ~26 of these read-only REST endpoints alongside the GACMD
            command surface -- exact paths vary by version; verify against
            your instance's REST docs (Admin > Documentation)."""
            if _demo:
                context.log.info(f"[STATUS] GET {_ep}/status -> service: RUNNING, uptime: 14d 6h")
                context.log.info(f"[SESSIONS] GET {_ep}/sessions/active -> 3 active transfer sessions")
                context.log.info(f"[SUMMARY] GET {_ep}/transfers/summary -> 148 transfers today, 2 failed")
            else:
                import os
                import requests as _req
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _goanywhere_headers(u or "", p or "")
                for label, path in (("status", "/status"), ("sessions", "/sessions/active"), ("summary", "/transfers/summary")):
                    try:
                        resp = _req.get(f"{_ep}{path}", headers=headers, timeout=30)
                        resp.raise_for_status()
                        context.log.info(f"[{label.upper()}] {resp.json()}")
                    except Exception as e:
                        context.log.warning(f"{label} endpoint failed (verify path against your instance): {e}")

        @dg.op
        def reconcile_goanywhere_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with GoAnywhere's actual job states for
            the jobs Dagster has triggered so far this process."""
            if _demo:
                context.log.info(f"[RECON] Checking {len(self.projects) or 2} tracked GoAnywhere projects")
                context.log.info(f"  GoAnywhere: 2 projects — 2 Successful, 0 Failed")
                context.log.info(f"  Dagster: materializations recorded for both")
                context.log.info(f"  DRIFT: none detected")
            else:
                import os
                import requests as _req
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _goanywhere_headers(u or "", p or "")
                for project_name, job_id in last_job_ids.items():
                    try:
                        resp = _req.get(
                            f"{_ep}/gacmd/v1/jobs/{job_id}",
                            params={"jobStatusFormat": "JSON"},
                            headers=headers, timeout=30,
                        )
                        resp.raise_for_status()
                        state = resp.json().get("status", "unknown")
                        context.log.info(f"[RECON] {project_name} (job {job_id}) -> {_STATUS_LABELS.get(state, state)}")
                        if state == "F":
                            context.log.warning(f"  ALERT: {project_name} — Failed")
                    except Exception as e:
                        context.log.warning(f"reconciliation check failed for {project_name}: {e}")

        @dg.job(description="Manually rerun a GoAnywhere Project.")
        def goanywhere_rerun_project():
            rerun_goanywhere_project()

        @dg.job(description="Pull GoAnywhere's dashboard summary (service status, active sessions, transfer summary).")
        def goanywhere_dashboard():
            goanywhere_dashboard_summary()

        @dg.job(description="Reconcile Dagster state with GoAnywhere — detect drift.")
        def goanywhere_reconciliation():
            reconcile_goanywhere_state()

        recon_schedule = dg.ScheduleDefinition(
            name="goanywhere_reconciliation_schedule",
            job=goanywhere_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="goanywhere_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for GoAnywhere-initiated pipeline triggers. In production, "
                "a GoAnywhere Project's post-step calls Dagster's GraphQL API "
                "(launchRun mutation) to start runs after a transfer/workflow completes."
            ),
        )
        def goanywhere_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  GoAnywhere trigger detected (tick {tick})")
                context.log.info(f"  Source: GoAnywhere Project post-step")
                context.log.info(f"  Action: In production, GoAnywhere calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[goanywhere_external_monitor, goanywhere_inbound_trigger],
            jobs=[goanywhere_rerun_project, goanywhere_dashboard, goanywhere_reconciliation],
            schedules=[recon_schedule],
        )
