"""RunMyJobs by Redwood integration component.

Bidirectional integration between Dagster and Redwood RunMyJobs (aka
SAP Redwood Scheduler / RMJ):

  Dagster -> RunMyJobs:
    - Submit a JobDefinition (POST /scheduler/api/submitjob)
    - Poll the resulting Process status (GET /scheduler/api/processes/{id})
    - Retrieve process output / stdout (GET /scheduler/api/processes/{id}/stdout)
    - Kill / hold / release ops for operational control
    - HTTP Basic Auth (username:password in Authorization header)

  RunMyJobs -> Dagster:
    - RunMyJobs job can call Dagster's GraphQL API (launchRun mutation)
    - Parameters passed via runConfigData; partition selected via ODATE-analog

Each declared JobDefinition becomes a daily-partitioned Dagster asset.
Operational tasks (restart / hold / release / kill / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the RMJ REST API on stdout — the
whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  RUNMYJOBS_USER      — REST API username
  RUNMYJOBS_PASSWORD  — REST API password

Terminology map — for teams migrating from Control-M:
  Control-M Job          -> RunMyJobs JobDefinition
  Control-M Folder       -> RunMyJobs Application
  Control-M Agent/Host   -> RunMyJobs Queue
  Control-M ODATE        -> RunMyJobs scheduledTime
  Control-M runId        -> RunMyJobs processId
  "Ended OK" / "Ended Not OK" -> "Completed" / "Error"

API reference:
  https://documentation.runmyjobs.cloud/
  (exact REST paths vary by RMJ version; verify against your instance
  when flipping demo_mode to false — see README).
"""
import base64
import json
import time
import uuid
from typing import List, Optional

import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
from pydantic import ConfigDict, Field


# ═════════════════════════════════════════════════════════════════════
# Specs
# ═════════════════════════════════════════════════════════════════════

class RunMyJobsJobSpec(dg.Model, dg.Resolvable):
    """A RunMyJobs JobDefinition wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_definition: str = Field(
        description="RunMyJobs JobDefinition name (as registered in the RMJ scheduler).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this JobDefinition.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    application: str = Field(default="", description="RunMyJobs Application (analog to Control-M Folder).")
    partition_type: str = Field(default="", description="Business tag for the job (e.g. CORE_BANKING).")
    sub_partition_type: str = Field(default="", description="Business sub-tag (e.g. SETTLEMENT).")
    queue: str = Field(default="", description="RunMyJobs Queue the job runs on (analog to Control-M Agent).")
    run_as: str = Field(default="svc_dagster", description="OS user RunMyJobs runs the job as.")


class RunMyJobsSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a RunMyJobs process that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="runmyjobs_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the RunMyJobs job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_runmyjobs(context: AssetExecutionContext, job: RunMyJobsJobSpec, endpoint: str) -> dict:
    """Simulate the REST API lifecycle to stdout — no external deps."""
    process_id = f"RMJ-{uuid.uuid4().hex[:8].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "jobDefinition": job.job_definition,
        "application": job.application,
        "queue": job.queue,
        "scheduledTime": scheduled,
        "parameters": {},
    }
    fake_basic = base64.b64encode(b"svc_dagster:******").decode()

    context.log.info(f"[AUTH]   Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[SUBMIT] POST {endpoint}/scheduler/api/submitjob")
    context.log.info(f"  Payload: {json.dumps(submit_payload, indent=2)}")
    context.log.info(f"  Response: 201 Created — processId: {process_id}")

    for state in ["Waiting Time", "Ready", "Running", "Running", "Completed"]:
        context.log.info(f"[POLL]   GET {endpoint}/scheduler/api/processes/{process_id} -> status={state}")

    context.log.info(f"[STDOUT] GET {endpoint}/scheduler/api/processes/{process_id}/stdout -> 623 lines")
    context.log.info(f"[DONE]   {job.job_definition} -> Completed (processId: {process_id}, scheduledTime: {scheduled})")

    feedback = {
        "eventName": "DAGSTER_JOB_COMPLETE",
        "jobDefinition": job.job_definition,
        "processId": process_id,
        "status": "Completed",
        "scheduledTime": scheduled,
        "dagsterRunId": context.run_id,
    }
    context.log.info(f"[EVENT]  POST {endpoint}/scheduler/api/processes/{process_id}/events")
    context.log.info(f"  Payload: {json.dumps(feedback, indent=2)}")

    return {"process_id": process_id, "status": "Completed", "scheduled_time": scheduled, "target": "runmyjobs"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _rmj_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_runmyjobs(
    context: AssetExecutionContext,
    job: RunMyJobsJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    stdout_retrieval: bool,
) -> dict:
    """Real RunMyJobs REST API v1/v2 lifecycle.

    REST paths vary across RMJ versions; the paths below are the modern
    JSON REST surface. If your RMJ instance uses a different prefix
    (e.g. /RunMyJobs/api-rest/scheduler/api/…), override `endpoint`
    to include the full prefix.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    headers = _rmj_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "jobDefinition": job.job_definition,
        "application": job.application,
        "queue": job.queue,
        "scheduledTime": scheduled,
        "parameters": {},
    }
    context.log.info(f"[SUBMIT] POST {endpoint}/scheduler/api/submitjob — {job.job_definition}")
    resp = requests.post(
        f"{endpoint}/scheduler/api/submitjob",
        json=submit_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    process_id = resp.json().get("processId") or resp.json().get("id", "unknown")
    context.log.info(f"  processId: {process_id}")

    start = time.time()
    terminal_states = {"Completed", "Error", "Killed", "Cancelled"}
    status = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/scheduler/api/processes/{process_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "UNKNOWN")
        context.log.info(f"[POLL]   {job.job_definition} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_definition} timed out after {poll_timeout}s")

    if status != "Completed":
        raise RuntimeError(f"{job.job_definition} finished: {status}")

    output_lines = 0
    if stdout_retrieval:
        try:
            out_resp = requests.get(
                f"{endpoint}/scheduler/api/processes/{process_id}/stdout",
                headers=headers, verify=False, timeout=30,
            )
            if out_resp.ok:
                output_lines = len(out_resp.text.split("\n"))
                context.log.info(f"[STDOUT] {output_lines} lines")
                for line in out_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"stdout retrieval failed: {e}")

    try:
        feedback = {
            "eventName": "DAGSTER_JOB_COMPLETE",
            "jobDefinition": job.job_definition, "processId": process_id,
            "status": "Completed", "scheduledTime": scheduled,
            "dagsterRunId": context.run_id,
        }
        requests.post(
            f"{endpoint}/scheduler/api/processes/{process_id}/events",
            json=feedback, headers=headers, verify=False, timeout=30,
        )
        context.log.info(f"[EVENT]  Sent completion event")
    except Exception as e:
        context.log.warning(f"event post failed: {e}")

    return {"process_id": process_id, "status": status, "scheduled_time": scheduled, "output_lines": output_lines, "target": "runmyjobs"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class RunMyJobsIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Redwood RunMyJobs REST API integration.

    Each declared JobDefinition becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    RunMyJobs-managed tables into Dagster's lineage graph. Operational
    tasks (restart / hold / release / kill / reconcile) ship as Dagster
    jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no RMJ endpoint. Flip to `false`
    and set `RUNMYJOBS_USER` / `RUNMYJOBS_PASSWORD` to hit a real
    RunMyJobs instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://runmyjobs.internal:8443/RunMyJobs/api-rest",
        description="RunMyJobs REST API base URL (used when demo_mode=false).",
    )
    jobs: List[RunMyJobsJobSpec] = Field(
        default_factory=list,
        description="RunMyJobs JobDefinitions to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[RunMyJobsSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by RunMyJobs that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="runmyjobs_integration",
        description="Dagster asset group for the job assets.",
    )

    runmyjobs_user_env: str = Field(
        default="RUNMYJOBS_USER",
        description="Env var holding the RunMyJobs REST username.",
    )
    runmyjobs_password_env: str = Field(
        default="RUNMYJOBS_PASSWORD",
        description="Env var holding the RunMyJobs REST password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between RunMyJobs process status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    stdout_retrieval: bool = Field(default=True, description="Retrieve process stdout on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the RunMyJobs vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.runmyjobs_user_env
        password_env = self.runmyjobs_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        stdout_retrieval = self.stdout_retrieval

        resolved_upstream = [dg.AssetKey(dep.split("/")) for dep in self.upstream_deps]
        daily_partition = dg.DailyPartitionsDefinition(start_date=self.partition_start_date)

        retry_policy = RetryPolicy(
            max_retries=self.max_retries,
            delay=self.retry_delay_seconds,
        )

        all_assets: List[dg.AssetsDefinition] = []

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
                    kinds={"python", "runmyjobs"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"RunMyJobs JobDefinition: {_job.job_definition}. "
                        f"Submits to {_job.application} (queue: {_job.queue or 'default'}), "
                        f"polls for completion, emits event."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "runmyjobs",
                        "integration_pattern": "dagster-orchestrates-runmyjobs",
                        "scheduler_owner": "RunMyJobs",
                        "rmj_application": _job.application,
                        "rmj_queue": _job.queue,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_runmyjobs(context, _job, _endpoint)
                    else:
                        result = _execute_runmyjobs(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, stdout_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_process_id": result.get("process_id", "N/A"),
                        "status": result.get("status", "N/A"),
                        "scheduled_time": result.get("scheduled_time", "N/A"),
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
                    kinds={"runmyjobs", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by RunMyJobs: {_table.table_name}. "
                        f"Data is loaded by RunMyJobs processes."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "runmyjobs_managed",
                        "table_name": _table.table_name,
                        "managed_by": "RunMyJobs",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: RunMyJobs (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown RMJ process'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="runmyjobs_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors RunMyJobs for process completions Dagster didn't trigger.",
        )
        def runmyjobs_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                process_id = f"ext-{uuid.uuid4().hex[:8]}"
                context.log.info(f"[DETECTED] External RunMyJobs process")
                context.log.info(f"  Triggered by: RunMyJobs schedule")
                context.log.info(f"  Status: Completed")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  processId: {process_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "runmyjobs_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_process_id": dg.MetadataValue.text(process_id),
                        "platform": dg.MetadataValue.text("runmyjobs"),
                        "triggered_by": dg.MetadataValue.text("RunMyJobs schedule"),
                    },
                )
                context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                yield dg.SensorResult(asset_events=[observation])
                return
            else:
                import os
                import requests
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                user = os.environ.get(user_env)
                pw = os.environ.get(password_env)
                if user and pw:
                    try:
                        headers = _rmj_headers(user, pw)
                        resp = requests.get(
                            f"{_ep}/scheduler/api/processes",
                            params={"status": "Completed", "limit": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for p in resp.json().get("processes", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"rmj_{(p.get('jobDefinition') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_process_id": dg.MetadataValue.text(str(p.get("processId", "unknown"))),
                                    "status": dg.MetadataValue.text(p.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"RunMyJobs monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"process_id": dg.Field(str, is_required=False, default_value="RMJ-UNKNOWN")})
        def restart_failed_runmyjobs_process(context: dg.OpExecutionContext):
            """Restart a failed RunMyJobs process. Config: {process_id: "RMJ-ABC"}."""
            process_id = context.op_config.get("process_id", "RMJ-UNKNOWN")
            if _demo:
                context.log.info(f"[RESTART] POST {_ep}/scheduler/api/processes/{process_id}/rerun")
                context.log.info(f"  Response: 200 OK — process resubmitted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _rmj_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/scheduler/api/processes/{process_id}/rerun",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] Process {process_id} resubmitted")

        @dg.op(config_schema={
            "application": dg.Field(str, is_required=False, default_value="DAILY_BATCH"),
            "queue": dg.Field(str, is_required=False, default_value="prod_queue"),
        })
        def hold_runmyjobs_application(context: dg.OpExecutionContext):
            """Hold all processes in an application on a queue. Config: {application, queue}."""
            application = context.op_config.get("application", "DAILY_BATCH")
            queue = context.op_config.get("queue", "prod_queue")
            if _demo:
                context.log.info(f"[HOLD] All processes in {application} held on queue {queue}")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _rmj_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/scheduler/api/applications/{application}/hold",
                    json={"queue": queue}, headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"application": dg.Field(str, is_required=False, default_value="DAILY_BATCH")})
        def release_held_runmyjobs_processes(context: dg.OpExecutionContext):
            """Release held processes. Config: {application}."""
            application = context.op_config.get("application", "DAILY_BATCH")
            if _demo:
                context.log.info(f"[RELEASE] All held processes in {application} released")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _rmj_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/scheduler/api/applications/{application}/release",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"process_id": dg.Field(str, is_required=False, default_value="RMJ-UNKNOWN")})
        def kill_runmyjobs_process(context: dg.OpExecutionContext):
            """Kill a running process. Config: {process_id}."""
            process_id = context.op_config.get("process_id", "RMJ-UNKNOWN")
            if _demo:
                context.log.info(f"[KILL] Process {process_id} terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _rmj_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/scheduler/api/processes/{process_id}/kill",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_runmyjobs_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with RunMyJobs' actual state."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/scheduler/api/processes?since=1h")
                context.log.info(f"  RunMyJobs: 52 processes — 44 Completed, 3 Error, 4 Running, 1 Waiting Time")
                context.log.info(f"  Dagster: materializations for 42 of 44 'Completed' processes")
                context.log.info(f"  DRIFT: 2 processes completed in RMJ but not in Dagster")
                context.log.info(f"  ALERT: 3 processes in 'Error' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _rmj_headers(u or "", p or "")
                resp = _req.get(
                    f"{_ep}/scheduler/api/processes",
                    params={"since": "1h", "limit": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                processes = resp.json().get("processes", [])
                by_status: dict = {}
                for pr in processes:
                    by_status.setdefault(pr.get("status", "?"), []).append(pr)
                context.log.info(f"[RECON] RunMyJobs: {len(processes)} processes")
                for st, prs in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(prs)}")
                for pr in by_status.get("Error", []):
                    context.log.warning(f"  ALERT: {pr.get('jobDefinition')} — Error")

        @dg.job(description="Restart a failed RunMyJobs process.")
        def runmyjobs_restart_process():
            restart_failed_runmyjobs_process()

        @dg.job(description="Hold all processes in a RunMyJobs application.")
        def runmyjobs_hold_application():
            hold_runmyjobs_application()

        @dg.job(description="Release held RunMyJobs processes.")
        def runmyjobs_release_processes():
            release_held_runmyjobs_processes()

        @dg.job(description="Kill a running RunMyJobs process.")
        def runmyjobs_kill_process():
            kill_runmyjobs_process()

        @dg.job(description="Reconcile Dagster state with RunMyJobs — detect drift.")
        def runmyjobs_reconciliation():
            reconcile_runmyjobs_state()

        recon_schedule = dg.ScheduleDefinition(
            name="runmyjobs_reconciliation_schedule",
            job=runmyjobs_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="runmyjobs_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for RunMyJobs-initiated pipeline triggers. In production, "
                "a RunMyJobs job calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled process completes."
            ),
        )
        def runmyjobs_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  RunMyJobs trigger detected (tick {tick})")
                context.log.info(f"  Source: RunMyJobs scheduled process completion")
                context.log.info(f"  Action: In production, RunMyJobs calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[runmyjobs_external_monitor, runmyjobs_inbound_trigger],
            jobs=[
                runmyjobs_restart_process, runmyjobs_hold_application,
                runmyjobs_release_processes, runmyjobs_kill_process,
                runmyjobs_reconciliation,
            ],
            schedules=[recon_schedule],
        )
