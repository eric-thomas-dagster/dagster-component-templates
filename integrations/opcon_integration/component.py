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

`demo_mode: true` (default) simulates the OpCon REST API on stdout — the
whole component runs end-to-end with zero external dependencies. OpCon is
on-prem workload-automation software with no public Docker image or
license-free trial, so the simulator is the only zero-license way to
smoke-test the whole surface.

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
import time
import uuid
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
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
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_opcon(context: AssetExecutionContext, job: OpConJobSpec, endpoint: str) -> dict:
    """Simulate the OpCon REST API lifecycle to stdout — no external deps."""
    job_id = str(uuid.uuid4().int % 90000 + 10000)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    action = "JOB:FORCERESTART" if job.force_restart else "JOB:START"

    context.log.info(f"[AUTH]   POST {endpoint}/tokens -> token=sim-***** (Authorization: Token <token>)")
    context.log.info(
        f"[ACTION] POST {endpoint}/api/scheduleactions {{action: {action!r}, "
        f"jobName: {job.job_name!r}, scheduleName: {job.schedule_name!r}, jobDate: {scheduled!r}}} -> jId={job_id}"
    )

    for status in ["Waiting", "Executing", "Executing", "Successful"]:
        context.log.info(f"[POLL]   GET {endpoint}/api/dailyjobs/{job_id}/status -> status={status}")

    context.log.info(f"[EVENTS] GET {endpoint}/api/dailyjobs/{job_id}/events -> 6 events")
    context.log.info(f"[DONE]   {job.job_name} -> Successful (jId: {job_id}, schedule: {job.schedule_name or 'default'})")

    return {"job_id": job_id, "status": "Successful", "scheduled_date": scheduled, "target": "opcon"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

_TERMINAL_STATUSES = {"Successful", "Failed", "Cancelled", "Skipped"}


def _opcon_token(endpoint: str, user: str, password: str) -> str:
    """Exchange credentials for a short-lived OpCon API token. Confirmed shape:
    POST /tokens with credentials, token comes back for use as
    `Authorization: Token <token>` (user tokens expire ~20 min per SMA's own
    getting-started doc; application tokens don't)."""
    import requests

    resp = requests.post(
        f"{endpoint}/tokens",
        json={"username": user, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    token = payload.get("token") or payload.get("Token") or payload.get("access_token")
    if not token:
        raise RuntimeError("OpCon /tokens response did not include a token field")
    return token


def _opcon_headers(token: str) -> dict:
    return {
        "Authorization": f"Token {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_opcon(
    context: AssetExecutionContext,
    job: OpConJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
) -> dict:
    """Real OpCon REST API lifecycle: token exchange, force-start/start a Daily
    Job via /api/scheduleactions, poll /api/dailyjobs/{jId}/status to a
    terminal state. The action-name + endpoint shape (POST /api/scheduleactions
    with action JOB:START / JOB:FORCERESTART; GET /api/dailyjobs/{jId}/status)
    is confirmed against SMA/Continuous's public docs; the exact request-body
    field names below are a best-effort match -- verify against your
    instance's own Swagger UI (`/api-doc/index.html`) before relying on this
    in production.
    """
    import os
    import requests

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    token = _opcon_token(endpoint, user, password)
    headers = _opcon_headers(token)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    action = "JOB:FORCERESTART" if job.force_restart else "JOB:START"

    action_payload = {
        "action": action,
        "jobName": job.job_name,
        "scheduleName": job.schedule_name or None,
        "machine": job.machine or None,
        "jobDate": scheduled,
    }
    context.log.info(f"[ACTION] POST {endpoint}/api/scheduleactions ({action})")
    resp = requests.post(
        f"{endpoint}/api/scheduleactions",
        json=action_payload, headers=headers, timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    job_id = str(
        payload.get("jId")
        or payload.get("jobId")
        or (payload.get("job") or {}).get("jId")
        or "unknown"
    )
    context.log.info(f"  jId: {job_id}")

    start = time.time()
    status = "Waiting"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/api/dailyjobs/{job_id}/status",
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        status_payload = resp.json()
        status = status_payload.get("status") or status_payload.get("Status") or "Waiting"
        context.log.info(f"[POLL]   {job.job_name} -> {status}")
        if status in _TERMINAL_STATUSES:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_name} timed out after {poll_timeout}s")

    if status != "Successful":
        raise RuntimeError(f"{job.job_name} finished: {status}")

    return {"job_id": job_id, "status": status, "scheduled_date": scheduled, "target": "opcon"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class OpConIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """SMA Technologies / Continuous OpCon REST API integration.

    Each declared OpCon Daily Job becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    OpCon-managed tables into Dagster's lineage graph. Operational tasks
    (force-restart / hold / release / cancel / reconcile) ship as Dagster
    jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no OpCon endpoint. Flip to `false`
    and set `OPCON_USER` / `OPCON_PASSWORD` to hit a real OpCon instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
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
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
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

    poll_interval_seconds: int = Field(default=10, description="Seconds between OpCon daily-job status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal state before failing.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the OpCon vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.opcon_user_env
        password_env = self.opcon_password_env
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

        for job in self.jobs:
            def _make_asset(
                _job=job,
                _endpoint=endpoint,
                _demo=demo_mode,
                _group=group,
                _upstream=resolved_upstream,
            ):
                @dg.asset(
                    name=_job.asset_name,
                    kinds={"python", "opcon"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"OpCon Job: {_job.job_name}. "
                        f"Force-starts on schedule {_job.schedule_name or 'default'} "
                        f"(machine: {_job.machine or 'default'}), polls for completion."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "opcon",
                        "integration_pattern": "dagster-orchestrates-opcon",
                        "scheduler_owner": "OpCon",
                        "opcon_schedule": _job.schedule_name,
                        "opcon_machine": _job.machine,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_opcon(context, _job, _endpoint)
                    else:
                        result = _execute_opcon(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout,
                        )
                    duration = round(time.time() - start_time, 2)
                    last_job_ids[_job.job_name] = result.get("job_id", "unknown")
                    context.add_output_metadata({
                        "external_job_id": result.get("job_id", "N/A"),
                        "status": result.get("status", "N/A"),
                        "scheduled_date": result.get("scheduled_date", "N/A"),
                        "duration_seconds": duration,
                        "demo_mode": _demo,
                    })

                return _asset_fn

            all_assets.append(_make_asset())

        source_assets: List[dg.AssetsDefinition] = []
        for table in self.source_tables:
            table_deps = [dg.AssetKey(table.produced_by)] if table.produced_by else []

            def _make_source(
                _table=table,
                _deps=table_deps,
            ):
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
                    context.log.info(f"  Managed by: OpCon (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown OpCon job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="opcon_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors OpCon for daily-job completions Dagster didn't trigger.",
        )
        def opcon_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                job_id = str(uuid.uuid4().int % 90000 + 10000)
                context.log.info(f"[DETECTED] External OpCon daily job")
                context.log.info(f"  Triggered by: OpCon schedule")
                context.log.info(f"  Status: Successful")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  jId: {job_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "opcon_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
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
                import os
                import requests
                user = os.environ.get(user_env)
                pw = os.environ.get(password_env)
                if user and pw:
                    try:
                        token = _opcon_token(_ep, user, pw)
                        headers = _opcon_headers(token)
                        resp = requests.get(
                            f"{_ep}/api/dailyjobs",
                            params={"status": "Successful", "limit": 20},
                            headers=headers, timeout=30,
                        )
                        resp.raise_for_status()
                        for e in (resp.json().get("items") or resp.json().get("dailyJobs") or [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"opcon_{(e.get('jobName') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(str(e.get("jId", "unknown"))),
                                    "status": dg.MetadataValue.text(e.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"OpCon monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
        def force_restart_opcon_job(context: dg.OpExecutionContext):
            """Force-restart an OpCon daily job, ignoring dependencies. Config: {job_id}."""
            job_id = context.op_config.get("job_id", "OPCON-UNKNOWN")
            if _demo:
                context.log.info(f"[FORCE_RESTART] POST {_ep}/api/scheduleactions (action: JOB:FORCERESTART, jId: {job_id})")
                context.log.info(f"  Response: 200 OK — job force-restarted")
            else:
                import os
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _opcon_token(_ep, u or "", p or "")
                headers = _opcon_headers(token)
                import requests as _req
                _req.post(
                    f"{_ep}/api/scheduleactions",
                    json={"action": "JOB:FORCERESTART", "jId": job_id},
                    headers=headers, timeout=30,
                ).raise_for_status()
                context.log.info(f"[FORCE_RESTART] job {job_id} force-restarted")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
        def hold_opcon_job(context: dg.OpExecutionContext):
            """Hold an OpCon daily job (JOB:HOLD -- verify this exact action name against
            your OpCon version's Swagger UI; confirmed action names are JOB:START /
            JOB:FORCERESTART, HOLD/RELEASE/CANCEL follow the same documented
            status-change-command convention but weren't individually confirmed).
            Config: {job_id}."""
            job_id = context.op_config.get("job_id", "OPCON-UNKNOWN")
            if _demo:
                context.log.info(f"[HOLD] POST {_ep}/api/scheduleactions (action: JOB:HOLD, jId: {job_id})")
                context.log.info(f"  Response: 200 OK — job held")
            else:
                import os
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _opcon_token(_ep, u or "", p or "")
                headers = _opcon_headers(token)
                import requests as _req
                _req.post(
                    f"{_ep}/api/scheduleactions",
                    json={"action": "JOB:HOLD", "jId": job_id},
                    headers=headers, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
        def release_opcon_job(context: dg.OpExecutionContext):
            """Release a held OpCon daily job (JOB:RELEASE -- see hold_opcon_job's
            verification note). Config: {job_id}."""
            job_id = context.op_config.get("job_id", "OPCON-UNKNOWN")
            if _demo:
                context.log.info(f"[RELEASE] POST {_ep}/api/scheduleactions (action: JOB:RELEASE, jId: {job_id})")
                context.log.info(f"  Response: 200 OK — job released")
            else:
                import os
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _opcon_token(_ep, u or "", p or "")
                headers = _opcon_headers(token)
                import requests as _req
                _req.post(
                    f"{_ep}/api/scheduleactions",
                    json={"action": "JOB:RELEASE", "jId": job_id},
                    headers=headers, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="OPCON-UNKNOWN")})
        def cancel_opcon_job(context: dg.OpExecutionContext):
            """Cancel a running OpCon daily job (JOB:CANCEL -- see hold_opcon_job's
            verification note). Config: {job_id}."""
            job_id = context.op_config.get("job_id", "OPCON-UNKNOWN")
            if _demo:
                context.log.info(f"[CANCEL] POST {_ep}/api/scheduleactions (action: JOB:CANCEL, jId: {job_id})")
                context.log.info(f"  Response: 200 OK — job cancelled")
            else:
                import os
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _opcon_token(_ep, u or "", p or "")
                headers = _opcon_headers(token)
                import requests as _req
                _req.post(
                    f"{_ep}/api/scheduleactions",
                    json={"action": "JOB:CANCEL", "jId": job_id},
                    headers=headers, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_opcon_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with OpCon's actual state, using the
            confirmed /api/dailyjobs/count_by_status endpoint."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/api/dailyjobs/count_by_status")
                context.log.info(f"  OpCon: 48 daily jobs — 41 Successful, 3 Failed, 3 Executing, 1 Held")
                context.log.info(f"  Dagster: materializations for 39 of 41 'Successful' jobs")
                context.log.info(f"  DRIFT: 2 jobs completed in OpCon but not in Dagster")
                context.log.info(f"  ALERT: 3 jobs in 'Failed' — manual review required")
            else:
                import os
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _opcon_token(_ep, u or "", p or "")
                headers = _opcon_headers(token)
                import requests as _req
                resp = _req.get(f"{_ep}/api/dailyjobs/count_by_status", headers=headers, timeout=30)
                resp.raise_for_status()
                counts = resp.json()
                context.log.info(f"[RECON] OpCon status counts: {counts}")
                failed = counts.get("Failed", 0) if isinstance(counts, dict) else 0
                if failed:
                    context.log.warning(f"  ALERT: {failed} job(s) in 'Failed' — manual review required")

        @dg.job(description="Force-restart an OpCon daily job, ignoring dependencies.")
        def opcon_force_restart_job():
            force_restart_opcon_job()

        @dg.job(description="Hold an OpCon daily job.")
        def opcon_hold_job():
            hold_opcon_job()

        @dg.job(description="Release a held OpCon daily job.")
        def opcon_release_job():
            release_opcon_job()

        @dg.job(description="Cancel a running OpCon daily job.")
        def opcon_cancel_job():
            cancel_opcon_job()

        @dg.job(description="Reconcile Dagster state with OpCon — detect drift.")
        def opcon_reconciliation():
            reconcile_opcon_state()

        recon_schedule = dg.ScheduleDefinition(
            name="opcon_reconciliation_schedule",
            job=opcon_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="opcon_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for OpCon-initiated pipeline triggers. In production, "
                "an OpCon job calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled job completes."
            ),
        )
        def opcon_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  OpCon trigger detected (tick {tick})")
                context.log.info(f"  Source: OpCon scheduled job completion")
                context.log.info(f"  Action: In production, OpCon calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[opcon_external_monitor, opcon_inbound_trigger],
            jobs=[
                opcon_force_restart_job, opcon_hold_job,
                opcon_release_job, opcon_cancel_job,
                opcon_reconciliation,
            ],
            schedules=[recon_schedule],
        )
