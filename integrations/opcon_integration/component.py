"""SMA Technologies / Continuous OpCon workload-automation integration component.

Bidirectional integration between Dagster and OpCon (SMA Technologies'
workload automation platform, now marketed under the Continuous brand):

  Dagster -> OpCon:
    - Exchange credentials for a token (POST /tokens)
    - Force-start a Daily Job (POST /api/scheduleactions, action JOB:FORCERESTART)
    - Poll the resulting Daily Job's status (GET /api/dailyjobs/{jId}/status)
    - Retrieve job history / events for observability
    - Token auth: `Authorization: Token <token>` header

  OpCon -> Dagster:
    - An OpCon Job can call Dagster's GraphQL API (launchRun mutation) as one
      of its own steps, or via a generic HTTP/REST job type
    - Parameters passed via run_config; partition selected via the job's own
      scheduled date

Each declared OpCon Daily Job becomes a daily-partitioned Dagster asset.

Client abstraction: `_OpConClient.submit`/`.poll`/`.status_counts` is the one
seam between "what the asset/op logic does" and "how it talks to OpCon." The
demo and real clients implement the identical interface, so the submit/poll
loop, terminal-state handling, and timeout logic all run through the SAME
code in both modes -- demo mode isn't a parallel reimplementation that
leaves the real polling logic unexercised.

`demo_mode: true` (default) uses `_DemoOpConClient`, which logs a simulated
OpCon REST lifecycle on stdout instead of making real HTTP calls. OpCon is
on-prem workload-automation software with no public Docker image or
license-free trial, so the demo client is the only zero-license way to
exercise the calling logic.

Environment variables (production mode only):
  OPCON_USER      — OpCon username for the /tokens credential exchange
  OPCON_PASSWORD  — OpCon password for the /tokens credential exchange

API reference (confirmed against SMA/Continuous's public, hosted OpenAPI
reference and community docs -- the richest-documented API in this
integration family; exact request-body field names for scheduleactions
weren't fully confirmed from public sources, so verify against your
instance's own `/api-doc/index.html` Swagger UI before going to production):
  https://help.smatechnologies.com/opcon/core/api/22-0
  https://community.continuous.com/rest-api-40/getting-started-with-the-opcon-api-469
"""
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

class OpConJobSpec(dg.Model, dg.Resolvable):
    """An OpCon Daily Job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_name: str = Field(
        description="OpCon Job name (as registered on the OpCon schedule).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    schedule_name: str = Field(default="", description="OpCon Schedule the job belongs to.")
    machine: str = Field(default="", description="OpCon Machine (agent) the job runs on.")
    department: str = Field(default="", description="OpCon Department the job is organized under.")
    force_restart: bool = Field(
        default=True,
        description="Use JOB:FORCERESTART (ignores dependencies, starts immediately) instead of JOB:START (honors dependencies).",
    )
    deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before THIS job runs -- per-job, not shared across every job.",
    )


class OpConSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by an OpCon Job that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="opcon_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the OpCon job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Client abstraction -- the one seam between calling logic and HTTP
# ═════════════════════════════════════════════════════════════════════

TERMINAL_STATUSES = {"Successful", "Failed", "Cancelled", "Skipped"}


def _opcon_headers(token: str) -> dict:
    return {
        "Authorization": f"Token {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


class OpConClient:
    """submit/poll/status_counts -- implemented identically by the demo and
    real clients, so the asset body's submit-then-poll-to-terminal loop (and
    the ops that reuse it) run through the same code regardless of
    demo_mode. Only the HTTP calls inside each method differ."""

    def submit(self, job: OpConJobSpec, scheduled_date: str) -> str:
        raise NotImplementedError

    def poll(self, job_id: str) -> str:
        raise NotImplementedError

    def status_counts(self) -> Dict[str, int]:
        raise NotImplementedError

    def action(self, action_name: str, job_id: str) -> None:
        raise NotImplementedError


class _DemoOpConClient(OpConClient):
    """Logs a simulated OpCon REST lifecycle. Job ids are seeded off
    (job_name, scheduled_date, call count) rather than random, so a given
    demo run is reproducible instead of printing a different id every time."""

    def __init__(self, log):
        self._log = log
        self._call_count = 0
        self._poll_sequences: Dict[str, Any] = {}

    def submit(self, job: OpConJobSpec, scheduled_date: str) -> str:
        self._call_count += 1
        rng = random.Random(f"{job.job_name}:{scheduled_date}:{self._call_count}")
        job_id = str(rng.randint(10000, 99999))
        action = "JOB:FORCERESTART" if job.force_restart else "JOB:START"

        self._log.info("[AUTH]   POST https://opcon.internal/tokens -> token=sim-***** (Authorization: Token <token>)")
        self._log.info(
            f"[ACTION] POST https://opcon.internal/api/scheduleactions {{action: {action!r}, "
            f"jobName: {job.job_name!r}, scheduleName: {job.schedule_name!r}, jobDate: {scheduled_date!r}}} -> jId={job_id}"
        )
        self._poll_sequences[job_id] = iter(["Waiting", "Executing", "Executing", "Successful"])
        return job_id

    def poll(self, job_id: str) -> str:
        status = next(self._poll_sequences.get(job_id, iter([])), "Successful")
        self._log.info(f"[POLL]   GET https://opcon.internal/api/dailyjobs/{job_id}/status -> status={status}")
        if status == "Successful":
            self._log.info(f"[EVENTS] GET https://opcon.internal/api/dailyjobs/{job_id}/events -> 6 events")
        return status

    def status_counts(self) -> Dict[str, int]:
        self._log.info("[RECON] GET https://opcon.internal/api/dailyjobs/count_by_status")
        return {"Successful": 41, "Failed": 3, "Executing": 3, "Held": 1}

    def action(self, action_name: str, job_id: str) -> None:
        self._log.info(f"[{action_name.split(':')[-1]}] POST https://opcon.internal/api/scheduleactions (action: {action_name}, jId: {job_id})")
        self._log.info("  Response: 200 OK (simulated)")


class _RealOpConClient(OpConClient):
    """Real OpCon REST API lifecycle. The action-name + endpoint shape
    (POST /api/scheduleactions with action JOB:START / JOB:FORCERESTART;
    GET /api/dailyjobs/{jId}/status; GET /api/dailyjobs/count_by_status) is
    confirmed against SMA/Continuous's public docs; the exact request-body
    field names below are a best-effort match -- verify against your
    instance's own Swagger UI (`/api-doc/index.html`) before relying on this
    in production. JOB:HOLD / JOB:RELEASE / JOB:CANCEL follow the same
    documented JOB:<VERB> convention as the confirmed START/FORCERESTART
    actions but weren't individually confirmed from public sources."""

    def __init__(self, endpoint: str, user: str, password: str, log):
        self._endpoint = endpoint
        self._log = log
        token = self._fetch_token(user, password)
        self._headers = _opcon_headers(token)

    def _fetch_token(self, user: str, password: str) -> str:
        import requests

        resp = requests.post(f"{self._endpoint}/tokens", json={"username": user, "password": password}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("token") or payload.get("Token") or payload.get("access_token")
        if not token:
            raise RuntimeError("OpCon /tokens response did not include a token field")
        return token

    def submit(self, job: OpConJobSpec, scheduled_date: str) -> str:
        import requests

        action = "JOB:FORCERESTART" if job.force_restart else "JOB:START"
        action_payload = {
            "action": action,
            "jobName": job.job_name,
            "scheduleName": job.schedule_name or None,
            "machine": job.machine or None,
            "jobDate": scheduled_date,
        }
        self._log.info(f"[ACTION] POST {self._endpoint}/api/scheduleactions ({action})")
        resp = requests.post(f"{self._endpoint}/api/scheduleactions", json=action_payload, headers=self._headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        job_id = str(
            payload.get("jId")
            or payload.get("jobId")
            or (payload.get("job") or {}).get("jId")
            or "unknown"
        )
        self._log.info(f"  jId: {job_id}")
        return job_id

    def poll(self, job_id: str) -> str:
        import requests

        resp = requests.get(f"{self._endpoint}/api/dailyjobs/{job_id}/status", headers=self._headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("status") or payload.get("Status") or "Waiting"

    def status_counts(self) -> Dict[str, int]:
        import requests

        resp = requests.get(f"{self._endpoint}/api/dailyjobs/count_by_status", headers=self._headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def action(self, action_name: str, job_id: str) -> None:
        import requests

        requests.post(
            f"{self._endpoint}/api/scheduleactions",
            json={"action": action_name, "jId": job_id}, headers=self._headers, timeout=30,
        ).raise_for_status()


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class OpConIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """SMA Technologies / Continuous OpCon REST API integration.

    Each declared OpCon Daily Job becomes a daily-partitioned Dagster asset
    with a retry policy. Everything beyond the job assets themselves -- the
    force-restart/hold/release/cancel/reconcile ops+jobs and the
    inbound-trigger sensor -- is opt-in (`generate_*` flags, default off):
    Dagster's own re-execution-from-failure (Dagster+) covers most of what a
    bespoke restart job would, and the inbound-trigger sensor only logs a
    simulated message in demo mode and does nothing in real mode until you
    wire a real webhook/source to it.

    `demo_mode: true` (default) swaps in `_DemoOpConClient` so the whole
    component runs end-to-end with no OpCon endpoint. Flip to `false` and
    set `OPCON_USER` / `OPCON_PASSWORD` to hit a real OpCon instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Use the demo client (logs a simulated REST lifecycle, no external calls) instead of the real REST client.",
    )
    endpoint: str = Field(
        default="https://opcon.internal",
        description="OpCon REST API base URL (used when demo_mode=false); scheduleactions/dailyjobs/tokens are relative to this.",
    )
    jobs: List[OpConJobSpec] = Field(
        default_factory=list,
        description="OpCon Daily Jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[OpConSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by OpCon that Dagster observes (adds to lineage).",
    )
    group_name: str = Field(
        default="opcon_integration",
        description="Dagster asset group for the job assets.",
    )

    opcon_user_env: str = Field(
        default="OPCON_USER",
        description="Env var holding the OpCon username for token exchange.",
    )
    opcon_password_env: str = Field(
        default="OPCON_PASSWORD",
        description="Env var holding the OpCon password for token exchange.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between OpCon daily-job status polls (real mode only -- demo mode doesn't sleep).")
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
        description="Build the opcon_external_execution_monitor sensor (observes OpCon-initiated runs Dagster didn't trigger). Cycles through every declared job, not just the first.",
    )
    generate_inbound_trigger_sensor: bool = Field(
        default=False,
        description="Build the opcon_inbound_trigger sensor. It only logs a simulated message in demo mode and does nothing in real mode until you wire a real webhook/event source -- off by default.",
    )
    generate_force_restart_job: bool = Field(
        default=False,
        description="Build the opcon_force_restart_job op-backed job. Off by default -- Dagster+'s own re-execution-from-failure covers most of what this would be used for.",
    )
    generate_hold_release_cancel_jobs: bool = Field(
        default=False,
        description="Build the opcon_hold_job / opcon_release_job / opcon_cancel_job op-backed jobs.",
    )
    generate_reconciliation: bool = Field(
        default=False,
        description="Build the reconciliation op/job and its hourly schedule.",
    )
    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the OpCon vs Dagster state reconciliation job (only used when generate_reconciliation=true).",
    )

    # ── translation hook ─────────────────────────────────────────────────
    translation: Annotated[
        Optional[TranslationFn[OpConJobSpec]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"job": data}),
    ] = Field(
        default=None,
        description=(
            "Optional per-job customization hook applied to each job's AssetSpec "
            "(e.g. `translation: {tags: {team: '{{ job.department }}'}}`). group_name/kinds/"
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
        user_env = self.opcon_user_env
        password_env = self.opcon_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        translation_fn = self.translation

        daily_partition = dg.DailyPartitionsDefinition(start_date=self.partition_start_date)
        retry_policy = RetryPolicy(max_retries=self.max_retries, delay=self.retry_delay_seconds)

        def _make_client(log) -> OpConClient:
            if demo_mode:
                return _DemoOpConClient(log)
            import os

            user = os.environ.get(user_env)
            password = os.environ.get(password_env)
            if not user or not password:
                raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")
            return _RealOpConClient(endpoint, user, password, log)

        all_assets: List[dg.AssetsDefinition] = []
        last_job_ids: Dict[str, str] = {}

        for job in self.jobs:
            def _make_asset(_job=job, _group=group):
                base_spec = dg.AssetSpec(
                    key=dg.AssetKey(_job.asset_name),
                    description=_job.description or (
                        f"OpCon Job: {_job.job_name}. "
                        f"{'Force-starts' if _job.force_restart else 'Starts'} on schedule "
                        f"{_job.schedule_name or 'default'} (machine: {_job.machine or 'default'}), "
                        f"polls for completion."
                    ),
                    group_name=_group,
                    deps=[dg.AssetKey(d.split("/")) for d in _job.deps],
                    kinds={"python", "opcon"},
                    tags={
                        "tier": "orchestration",
                        "domain": "opcon",
                        "integration_pattern": "dagster-orchestrates-opcon",
                        "scheduler_owner": "opcon",
                        "deployment_mode": "hybrid",
                    },
                    metadata={
                        "opcon_schedule": _job.schedule_name,
                        "opcon_machine": _job.machine,
                    },
                )
                final_spec = translation_fn(base_spec, _job) if translation_fn else base_spec

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

                    job_id = client.submit(_job, scheduled)
                    start = time.time()
                    status = "Waiting"
                    while time.time() - start < poll_timeout:
                        status = client.poll(job_id)
                        if status in TERMINAL_STATUSES:
                            break
                        if not demo_mode:
                            time.sleep(poll_interval)
                    else:
                        raise TimeoutError(f"{_job.job_name} timed out after {poll_timeout}s")

                    if status != "Successful":
                        raise RuntimeError(f"{_job.job_name} finished: {status}")

                    duration = round(time.time() - start_time, 2)
                    last_job_ids[_job.job_name] = job_id
                    context.add_output_metadata({
                        "external_job_id": job_id,
                        "status": status,
                        "scheduled_date": scheduled,
                        "duration_seconds": duration,
                        "demo_mode": demo_mode,
                    })

                return _asset_fn

            all_assets.append(_make_asset())

        source_assets: List[dg.AssetsDefinition] = []
        for table in self.source_tables:
            table_deps = [dg.AssetKey(table.produced_by)] if table.produced_by else []

            def _make_source(_table=table, _deps=table_deps):
                @dg.asset(
                    name=_table.asset_name,
                    kinds={"opcon", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by OpCon: {_table.table_name}. "
                        f"Data is loaded by an OpCon job."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "opcon_managed",
                        "table_name": _table.table_name,
                        "managed_by": "OpCon",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info("  Managed by: OpCon (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown OpCon job'}")
                    context.log.info("  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        sensors: List[Any] = []
        jobs: List[Any] = []
        schedules: List[Any] = []

        if self.generate_external_monitor_sensor:
            @dg.sensor(
                name="opcon_external_execution_monitor",
                minimum_interval_seconds=60,
                default_status=dg.DefaultSensorStatus.STOPPED,
                description="Monitors OpCon for daily-job completions Dagster didn't trigger. Cycles through every declared job.",
            )
            def opcon_external_monitor(context: dg.SensorEvaluationContext):
                last_cursor = context.cursor or "0|"
                tick = int(last_cursor.split("|")[0]) + 1

                if not self.jobs:
                    context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                    return

                job = self.jobs[(tick - 1) % len(self.jobs)]

                if demo_mode:
                    rng = random.Random(f"{job.job_name}:{tick}")
                    job_id = str(rng.randint(10000, 99999))
                    context.log.info("[DETECTED] External OpCon daily job")
                    context.log.info(f"  Job: {job.job_name}")
                    context.log.info("  Triggered by: OpCon schedule")
                    context.log.info("  Status: Successful")
                    context.log.info(f"  jId: {job_id}")
                    context.log.info("  Dagster was NOT the orchestrator — recording observation")

                    observation = dg.AssetObservation(
                        asset_key=dg.AssetKey(job.asset_name),
                        metadata={
                            "external_job_id": dg.MetadataValue.text(job_id),
                            "platform": dg.MetadataValue.text("opcon"),
                            "triggered_by": dg.MetadataValue.text("OpCon schedule"),
                        },
                    )
                    context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                    yield dg.SensorResult(asset_events=[observation])
                    return
                else:
                    job_id = last_job_ids.get(job.job_name)
                    if job_id:
                        try:
                            client = _make_client(context.log)
                            status = client.poll(job_id)
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(job.asset_name),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(job_id),
                                    "status": dg.MetadataValue.text(status),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                        except Exception as e:
                            context.log.warning(f"OpCon monitor failed: {e}")

                context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

            sensors.append(opcon_external_monitor)

        if self.generate_inbound_trigger_sensor:
            @dg.sensor(
                name="opcon_inbound_trigger",
                minimum_interval_seconds=300,
                default_status=dg.DefaultSensorStatus.STOPPED,
                description=(
                    "Stub: confirms the wiring point for an OpCon-initiated pipeline trigger. "
                    "In production, an OpCon job calls Dagster's GraphQL API (launchRun mutation) "
                    "directly -- this sensor does nothing in real mode until you replace its body "
                    "with a real event source."
                ),
            )
            def opcon_inbound_trigger(context: dg.SensorEvaluationContext):
                last_cursor = int(context.cursor) if context.cursor else 0
                tick = last_cursor + 1
                if demo_mode and tick % 3 == 0:
                    context.log.info(f"[INBOUND]  OpCon trigger detected (tick {tick})")
                    context.log.info("  Source: OpCon scheduled job completion")
                    context.log.info("  Action: In production, OpCon calls:")
                    context.log.info("    POST https://dagster.cloud/graphql")
                    context.log.info("    mutation { launchRun(executionParams: { ... }) }")
                    context.log.info("  This sensor confirms the inbound trigger was received")
                context.update_cursor(str(tick))

            sensors.append(opcon_inbound_trigger)

        if self.generate_force_restart_job:
            @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
            def force_restart_opcon_job(context: dg.OpExecutionContext):
                """Force-restart an OpCon daily job, ignoring dependencies. Config: {job_id}."""
                job_id = context.op_config.get("job_id", "OPCON-UNKNOWN")
                client = _make_client(context.log)
                client.action("JOB:FORCERESTART", job_id)

            @dg.job(description="Force-restart an OpCon daily job, ignoring dependencies.")
            def opcon_force_restart_job():
                force_restart_opcon_job()

            jobs.append(opcon_force_restart_job)

        if self.generate_hold_release_cancel_jobs:
            @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
            def hold_opcon_job(context: dg.OpExecutionContext):
                """Hold an OpCon daily job (JOB:HOLD -- verify this exact action name against
                your OpCon version's Swagger UI; only JOB:START / JOB:FORCERESTART were
                individually confirmed from public sources). Config: {job_id}."""
                client = _make_client(context.log)
                client.action("JOB:HOLD", context.op_config.get("job_id", "OPCON-UNKNOWN"))

            @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
            def release_opcon_job(context: dg.OpExecutionContext):
                """Release a held OpCon daily job (JOB:RELEASE -- see hold_opcon_job's
                verification note). Config: {job_id}."""
                client = _make_client(context.log)
                client.action("JOB:RELEASE", context.op_config.get("job_id", "OPCON-UNKNOWN"))

            @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
            def cancel_opcon_job(context: dg.OpExecutionContext):
                """Cancel a running OpCon daily job (JOB:CANCEL -- see hold_opcon_job's
                verification note). Config: {job_id}."""
                client = _make_client(context.log)
                client.action("JOB:CANCEL", context.op_config.get("job_id", "OPCON-UNKNOWN"))

            @dg.job(description="Hold an OpCon daily job.")
            def opcon_hold_job():
                hold_opcon_job()

            @dg.job(description="Release a held OpCon daily job.")
            def opcon_release_job():
                release_opcon_job()

            @dg.job(description="Cancel a running OpCon daily job.")
            def opcon_cancel_job():
                cancel_opcon_job()

            jobs.extend([opcon_hold_job, opcon_release_job, opcon_cancel_job])

        if self.generate_reconciliation:
            @dg.op
            def reconcile_opcon_state(context: dg.OpExecutionContext):
                """Compare Dagster's view with OpCon's actual state, using the
                confirmed /api/dailyjobs/count_by_status endpoint."""
                client = _make_client(context.log)
                counts = client.status_counts()
                context.log.info(f"[RECON] OpCon status counts: {counts}")
                failed = counts.get("Failed", 0) if isinstance(counts, dict) else 0
                if failed:
                    context.log.warning(f"  ALERT: {failed} job(s) in 'Failed' — manual review required")

            @dg.job(description="Reconcile Dagster state with OpCon — detect drift.")
            def opcon_reconciliation():
                reconcile_opcon_state()

            jobs.append(opcon_reconciliation)
            schedules.append(dg.ScheduleDefinition(
                name="opcon_reconciliation_schedule",
                job=opcon_reconciliation,
                cron_schedule=self.reconciliation_cron,
                default_status=dg.DefaultScheduleStatus.STOPPED,
            ))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=sensors,
            jobs=jobs,
            schedules=schedules,
        )
