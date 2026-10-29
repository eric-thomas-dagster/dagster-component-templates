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

Client abstraction: `_GoAnywhereClient.submit`/`.poll`/`.dashboard_summary`
is the one seam between "what the asset/op logic does" and "how it talks to
GoAnywhere." The demo and real clients implement the identical interface, so
the submit/poll loop, terminal-state handling, and timeout logic all run
through the SAME code in both modes -- demo mode isn't a parallel
reimplementation that leaves the real polling logic unexercised.

`demo_mode: true` (default) uses `_DemoGoAnywhereClient`, which logs a
simulated GACMD lifecycle on stdout instead of making real HTTP calls --
the whole component runs end-to-end with zero external dependencies.
GoAnywhere MFT does ship Docker images (fortrallc/goanywhere-mft on Docker
Hub), but the admin console and REST API don't do anything useful until
activated with a trial or paid license key (Fortra doesn't publish a
license-free mode), so the demo client is still the only zero-license way
to exercise the calling logic.

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
import random
import time
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver
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
    deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before THIS project runs -- per-project, not shared across every project.",
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
# Client abstraction -- the one seam between calling logic and HTTP
# ═════════════════════════════════════════════════════════════════════

_STATUS_LABELS = {
    "Q": "Queued", "A": "Active", "P": "Paused",
    "S": "Successful", "F": "Failed", "C": "Cancelled",
}
TERMINAL_STATES = {"S", "F", "C"}


def _goanywhere_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


class GoAnywhereClient:
    """submit/poll/dashboard_summary -- implemented identically by the demo
    and real clients, so the asset body's submit-then-poll-to-terminal loop
    (and the ops that reuse it) run through the same code regardless of
    demo_mode. Only the HTTP calls inside each method differ."""

    def submit(self, project: GoAnywhereProjectSpec, scheduled_date: str) -> str:
        raise NotImplementedError

    def poll(self, job_id: str) -> str:
        raise NotImplementedError

    def dashboard_summary(self) -> Dict[str, Any]:
        raise NotImplementedError


class _DemoGoAnywhereClient(GoAnywhereClient):
    """Logs a simulated GACMD lifecycle. Job ids are seeded off
    (project, scheduled_date, call count) rather than random, so a given
    demo run is reproducible instead of printing a different id every time."""

    def __init__(self, log):
        self._log = log
        self._call_count = 0
        self._poll_sequences: Dict[str, Any] = {}

    def submit(self, project: GoAnywhereProjectSpec, scheduled_date: str) -> str:
        self._call_count += 1
        rng = random.Random(f"{project.project_name}:{scheduled_date}:{self._call_count}")
        job_id = str(rng.randint(100000, 999999))
        run_payload = {
            "runParameters": {
                "project": project.project_name,
                "domain": project.domain or None,
                "jobName": project.job_name or project.project_name,
                "jobQueue": project.job_queue or None,
                "mode": project.mode,
                "priority": project.priority,
                "variables": {**project.variables, "SCHEDULED_DATE": scheduled_date},
            },
        }
        fake_basic = base64.b64encode(b"svc_dagster:******").decode()
        self._log.info(f"[AUTH]   Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
        self._log.info(f"[RUN]    POST https://goanywhere.internal/goanywhere/rest/gacmd/v1/projects -> jobId={job_id}")
        self._log.info(f"  Body: {json.dumps(run_payload, indent=2, default=str)}")
        self._poll_sequences[job_id] = iter(["Q", "A", "A", "S"])
        return job_id

    def poll(self, job_id: str) -> str:
        state = next(self._poll_sequences.get(job_id, iter([])), "S")
        self._log.info(
            f"[POLL]   GET https://goanywhere.internal/goanywhere/rest/gacmd/v1/jobs/{job_id}"
            f"?jobStatusFormat=JSON -> status={state}"
        )
        return state

    def dashboard_summary(self) -> Dict[str, Any]:
        self._log.info("[STATUS] GET .../status -> service: RUNNING, uptime: 14d 6h")
        self._log.info("[SESSIONS] GET .../sessions/active -> 3 active transfer sessions")
        self._log.info("[SUMMARY] GET .../transfers/summary -> 148 transfers today, 2 failed")
        return {"status": "RUNNING", "active_sessions": 3, "transfers_today": 148, "transfers_failed": 2}


class _RealGoAnywhereClient(GoAnywhereClient):
    """Real GACMD REST API lifecycle. Confirmed shape (Fortra product docs +
    forum posts, no single hosted API reference) -- `POST .../gacmd/v1/projects`
    with a `runParameters` body returns a jobId; `GET .../gacmd/v1/jobs/{jobId}
    ?jobStatusFormat=JSON` returns one of Q/A/P/S/F/C. Verify field names
    against your instance's built-in REST docs (Admin > Documentation) before
    relying on this in production -- Fortra doesn't publish a single public
    reference and field names have shifted across GoAnywhere releases."""

    def __init__(self, endpoint: str, user: str, password: str, log):
        self._endpoint = endpoint
        self._headers = _goanywhere_headers(user, password)
        self._log = log

    def submit(self, project: GoAnywhereProjectSpec, scheduled_date: str) -> str:
        import requests

        run_payload = {
            "runParameters": {
                "project": project.project_name,
                "domain": project.domain or None,
                "jobName": project.job_name or project.project_name,
                "jobQueue": project.job_queue or None,
                "mode": project.mode,
                "priority": project.priority,
                "variables": {**project.variables, "SCHEDULED_DATE": scheduled_date},
            },
        }
        self._log.info(f"[RUN]    POST {self._endpoint}/gacmd/v1/projects")
        resp = requests.post(f"{self._endpoint}/gacmd/v1/projects", json=run_payload, headers=self._headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        job_id = str(
            payload.get("jobId")
            or payload.get("JobID")
            or (payload.get("job") or {}).get("id")
            or "unknown"
        )
        self._log.info(f"  jobId: {job_id}")
        return job_id

    def poll(self, job_id: str) -> str:
        import requests

        resp = requests.get(
            f"{self._endpoint}/gacmd/v1/jobs/{job_id}",
            params={"jobStatusFormat": "JSON"}, headers=self._headers, timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("status") or payload.get("Status") or payload.get("jobStatus") or "Q"

    def dashboard_summary(self) -> Dict[str, Any]:
        import requests

        out: Dict[str, Any] = {}
        for label, path in (("status", "/status"), ("sessions", "/sessions/active"), ("summary", "/transfers/summary")):
            try:
                resp = requests.get(f"{self._endpoint}{path}", headers=self._headers, timeout=30)
                resp.raise_for_status()
                out[label] = resp.json()
                self._log.info(f"[{label.upper()}] {out[label]}")
            except Exception as e:
                self._log.warning(f"{label} endpoint failed (verify path against your instance): {e}")
        return out


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class GoAnywhereIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fortra GoAnywhere MFT GACMD REST API integration.

    Each declared GoAnywhere Project becomes a daily-partitioned Dagster
    asset with a retry policy. Everything beyond the project assets
    themselves -- the rerun/dashboard/reconciliation ops+jobs and the
    inbound-trigger sensor -- is opt-in (`generate_*` flags, default off):
    Dagster's own re-execution-from-failure (Dagster+) covers most of what
    a bespoke rerun job would, and the inbound-trigger sensor only logs a
    simulated message in demo mode and does nothing in real mode until you
    wire a real webhook/source to it, so it shouldn't look like working
    functionality by default.

    `demo_mode: true` (default) swaps in `_DemoGoAnywhereClient` so the
    whole component runs end-to-end with no GoAnywhere endpoint. Flip to
    `false` and set `GOANYWHERE_USER` / `GOANYWHERE_PASSWORD` to hit a real
    GoAnywhere MFT instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Use the demo client (logs a simulated GACMD lifecycle, no external calls) instead of the real REST client.",
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

    poll_interval_seconds: int = Field(default=10, description="Seconds between GoAnywhere job status polls (real mode only -- demo mode doesn't sleep).")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal state before failing.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    # ── opt-in scaffold, all off by default ─────────────────────────────
    generate_external_monitor_sensor: bool = Field(
        default=True,
        description="Build the goanywhere_external_execution_monitor sensor (observes GoAnywhere-initiated runs Dagster didn't trigger). Cycles through every declared project, not just the first.",
    )
    generate_inbound_trigger_sensor: bool = Field(
        default=False,
        description="Build the goanywhere_inbound_trigger sensor. It only logs a simulated message in demo mode and does nothing in real mode until you wire a real webhook/event source to it -- off by default so it doesn't look like working functionality.",
    )
    generate_rerun_job: bool = Field(
        default=False,
        description="Build the goanywhere_rerun_project op-backed job. Off by default -- Dagster+'s own re-execution-from-failure covers most of what this would be used for.",
    )
    generate_dashboard_job: bool = Field(
        default=False,
        description="Build the goanywhere_dashboard op-backed job (service status / active sessions / transfer summary).",
    )
    generate_reconciliation: bool = Field(
        default=False,
        description="Build the reconciliation op/job and its hourly schedule.",
    )
    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the GoAnywhere vs Dagster state reconciliation job (only used when generate_reconciliation=true).",
    )

    # ── translation hook ─────────────────────────────────────────────────
    translation: Annotated[
        Optional[TranslationFn[GoAnywhereProjectSpec]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"project": data}),
    ] = Field(
        default=None,
        description=(
            "Optional per-project customization hook applied to each project's AssetSpec "
            "(e.g. `translation: {tags: {team: '{{ project.domain }}'}}`). group_name/kinds/"
            "deps/description/owners are REPLACED by whatever you set; tags/metadata are "
            "MERGED onto the component's defaults (Dagster framework behavior) -- to drop a "
            "default tag/metadata key entirely, override it to an explicit placeholder rather "
            "than expecting it to disappear."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.goanywhere_user_env
        password_env = self.goanywhere_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        translation_fn = self.translation

        daily_partition = dg.DailyPartitionsDefinition(start_date=self.partition_start_date)
        retry_policy = RetryPolicy(max_retries=self.max_retries, delay=self.retry_delay_seconds)

        def _make_client(log) -> GoAnywhereClient:
            if demo_mode:
                return _DemoGoAnywhereClient(log)
            import os

            user = os.environ.get(user_env)
            password = os.environ.get(password_env)
            if not user or not password:
                raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")
            return _RealGoAnywhereClient(endpoint, user, password, log)

        all_assets: List[dg.AssetsDefinition] = []
        last_job_ids: Dict[str, str] = {}

        for project in self.projects:
            def _make_asset(_project=project, _group=group):
                base_spec = dg.AssetSpec(
                    key=dg.AssetKey(_project.asset_name),
                    description=_project.description or (
                        f"GoAnywhere Project: {_project.project_name}. "
                        f"Runs via GACMD REST (queue: {_project.job_queue or 'default'}), "
                        f"polls for a terminal job status."
                    ),
                    group_name=_group,
                    deps=[dg.AssetKey(d.split("/")) for d in _project.deps],
                    kinds={"python", "goanywhere"},
                    tags={
                        "tier": "orchestration",
                        "domain": "goanywhere",
                        "integration_pattern": "dagster-orchestrates-goanywhere",
                        "scheduler_owner": "goanywhere-mft",
                        "deployment_mode": "hybrid",
                    },
                    metadata={
                        "goanywhere_domain": _project.domain,
                        "goanywhere_queue": _project.job_queue,
                    },
                )
                final_spec = translation_fn(base_spec, _project) if translation_fn else base_spec

                @dg.asset(
                    key=final_spec.key,
                    description=final_spec.description,
                    group_name=final_spec.group_name,
                    deps=list(final_spec.deps or []),
                    tags=dict(final_spec.tags or {}),
                    kinds=set(final_spec.kinds or []),
                    metadata=dict(final_spec.metadata or {}),
                    owners=list(final_spec.owners or []),
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
                    client = _make_client(context.log)

                    job_id = client.submit(_project, scheduled)
                    start = time.time()
                    state = "Q"
                    while time.time() - start < poll_timeout:
                        state = client.poll(job_id)
                        if state in TERMINAL_STATES:
                            break
                        if not demo_mode:
                            time.sleep(poll_interval)
                    else:
                        raise TimeoutError(f"{_project.project_name} timed out after {poll_timeout}s")

                    if state != "S":
                        raise RuntimeError(f"{_project.project_name} finished: {_STATUS_LABELS.get(state, state)}")

                    duration = round(time.time() - start_time, 2)
                    last_job_ids[_project.project_name] = job_id
                    context.add_output_metadata({
                        "external_job_id": job_id,
                        "state": _STATUS_LABELS.get(state, state),
                        "scheduled_date": scheduled,
                        "duration_seconds": duration,
                        "demo_mode": demo_mode,
                    })

                return _asset_fn

            all_assets.append(_make_asset())

        source_assets: List[dg.AssetsDefinition] = []
        for source_file in self.source_files:
            file_deps = [dg.AssetKey(source_file.produced_by)] if source_file.produced_by else []

            def _make_source(_file=source_file, _deps=file_deps):
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
                    context.log.info("  Managed by: GoAnywhere MFT (external)")
                    context.log.info(f"  Produced by: {_file.produced_by or 'unknown GoAnywhere project'}")
                    context.log.info("  This asset represents the landed data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        sensors: List[Any] = []
        jobs: List[Any] = []
        schedules: List[Any] = []

        if self.generate_external_monitor_sensor:
            @dg.sensor(
                name="goanywhere_external_execution_monitor",
                minimum_interval_seconds=60,
                default_status=dg.DefaultSensorStatus.STOPPED,
                description="Monitors GoAnywhere for project runs Dagster didn't trigger (e.g. a file-arrival Trigger fired one). Cycles through every declared project.",
            )
            def goanywhere_external_monitor(context: dg.SensorEvaluationContext):
                last_cursor = context.cursor or "0|"
                tick = int(last_cursor.split("|")[0]) + 1

                if not self.projects:
                    context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                    return

                project = self.projects[(tick - 1) % len(self.projects)]

                if demo_mode:
                    rng = random.Random(f"{project.project_name}:{tick}")
                    job_id = str(rng.randint(100000, 999999))
                    context.log.info("[DETECTED] External GoAnywhere job")
                    context.log.info(f"  Project: {project.project_name}")
                    context.log.info("  Triggered by: GoAnywhere file-arrival Trigger")
                    context.log.info("  State: Successful")
                    context.log.info(f"  jobId: {job_id}")
                    context.log.info("  Dagster was NOT the orchestrator — recording observation")

                    observation = dg.AssetObservation(
                        asset_key=dg.AssetKey(project.asset_name),
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
                    job_id = last_job_ids.get(project.project_name)
                    if job_id:
                        try:
                            client = _make_client(context.log)
                            state = client.poll(job_id)
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(project.asset_name),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(job_id),
                                    "state": dg.MetadataValue.text(_STATUS_LABELS.get(state, state)),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                        except Exception as e:
                            context.log.warning(f"GoAnywhere monitor failed: {e}")

                context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

            sensors.append(goanywhere_external_monitor)

        if self.generate_inbound_trigger_sensor:
            @dg.sensor(
                name="goanywhere_inbound_trigger",
                minimum_interval_seconds=300,
                default_status=dg.DefaultSensorStatus.STOPPED,
                description=(
                    "Stub: confirms the wiring point for a GoAnywhere-initiated pipeline trigger. "
                    "In production, a GoAnywhere Project's post-step (or a file-arrival Trigger) "
                    "calls Dagster's GraphQL API (launchRun mutation) directly -- this sensor does "
                    "nothing in real mode until you replace its body with a real event source."
                ),
            )
            def goanywhere_inbound_trigger(context: dg.SensorEvaluationContext):
                last_cursor = int(context.cursor) if context.cursor else 0
                tick = last_cursor + 1
                if demo_mode and tick % 3 == 0:
                    context.log.info(f"[INBOUND]  GoAnywhere trigger detected (tick {tick})")
                    context.log.info("  Source: GoAnywhere Project post-step")
                    context.log.info("  Action: In production, GoAnywhere calls:")
                    context.log.info("    POST https://dagster.cloud/graphql")
                    context.log.info("    mutation { launchRun(executionParams: { ... }) }")
                    context.log.info("  This sensor confirms the inbound trigger was received")
                context.update_cursor(str(tick))

            sensors.append(goanywhere_inbound_trigger)

        if self.generate_rerun_job:
            @dg.op(config_schema={"project_name": dg.Field(str, is_required=True)})
            def rerun_goanywhere_project(context: dg.OpExecutionContext):
                """Manually rerun a GoAnywhere Project by name. Config: {project_name}."""
                project_name = context.op_config["project_name"]
                matching = next((p for p in self.projects if p.project_name == project_name), None)
                if matching is None:
                    raise ValueError(f"No declared project named {project_name!r}")
                client = _make_client(context.log)
                job_id = client.submit(matching, time.strftime("%Y-%m-%d"))
                context.log.info(f"[RERUN]  {project_name} resubmitted: jobId={job_id}")

            @dg.job(description="Manually rerun a GoAnywhere Project.")
            def goanywhere_rerun_project():
                rerun_goanywhere_project()

            jobs.append(goanywhere_rerun_project)

        if self.generate_dashboard_job:
            @dg.op
            def goanywhere_dashboard_summary(context: dg.OpExecutionContext):
                """Pull GoAnywhere's dashboard-style info endpoints (service status,
                active sessions, transfer summary) for observability."""
                client = _make_client(context.log)
                client.dashboard_summary()

            @dg.job(description="Pull GoAnywhere's dashboard summary (service status, active sessions, transfer summary).")
            def goanywhere_dashboard():
                goanywhere_dashboard_summary()

            jobs.append(goanywhere_dashboard)

        if self.generate_reconciliation:
            @dg.op
            def reconcile_goanywhere_state(context: dg.OpExecutionContext):
                """Compare Dagster's view with GoAnywhere's actual job states for
                the jobs Dagster has triggered so far this process."""
                client = _make_client(context.log)
                if not last_job_ids:
                    context.log.info("[RECON] No tracked GoAnywhere jobs yet this process.")
                    return
                for project_name, job_id in last_job_ids.items():
                    try:
                        state = client.poll(job_id)
                        context.log.info(f"[RECON] {project_name} (job {job_id}) -> {_STATUS_LABELS.get(state, state)}")
                        if state == "F":
                            context.log.warning(f"  ALERT: {project_name} — Failed")
                    except Exception as e:
                        context.log.warning(f"reconciliation check failed for {project_name}: {e}")

            @dg.job(description="Reconcile Dagster state with GoAnywhere — detect drift.")
            def goanywhere_reconciliation():
                reconcile_goanywhere_state()

            jobs.append(goanywhere_reconciliation)
            schedules.append(dg.ScheduleDefinition(
                name="goanywhere_reconciliation_schedule",
                job=goanywhere_reconciliation,
                cron_schedule=self.reconciliation_cron,
                default_status=dg.DefaultScheduleStatus.STOPPED,
            ))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=sensors,
            jobs=jobs,
            schedules=schedules,
        )
