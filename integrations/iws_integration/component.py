"""IBM Workload Scheduler (IWS / TWS) integration component.

Bidirectional integration between Dagster and IBM Workload Scheduler
(formerly Tivoli Workload Scheduler / TWS; distributed engine and z/OS
engine share a similar REST surface):

  Dagster -> IWS:
    - Submit a job / jobstream (POST /plan/current/jobstream)
    - Poll job status (GET /plan/current/job/{jobId})
    - Retrieve stdlist / job log (GET /plan/current/job/{jobId}/stdlist)
    - Kill / hold / release / rerun ops for operational control
    - HTTP Basic Auth (username:password in Authorization header)

  IWS -> Dagster:
    - IWS job can call Dagster's GraphQL API (launchRun mutation)
    - Parameters passed via runConfigData; partition selected via IA-date analog

Each declared IWS Job becomes a daily-partitioned Dagster asset.
Operational tasks (rerun / hold / release / kill / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the IWS REST API on stdout — the
whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  IWS_USER      — REST API username
  IWS_PASSWORD  — REST API password

Terminology map — for teams migrating from Control-M or RunMyJobs:
  Control-M Job          -> IWS Job                       (RMJ JobDefinition)
  Control-M Folder       -> IWS Application (JobStream)   (RMJ Application)
  Control-M Agent/Host   -> IWS Workstation               (RMJ Queue)
  Control-M ODATE        -> IWS scheduled time / IA       (RMJ scheduledTime)
  Control-M runId        -> IWS jobId                     (RMJ processId)
  "Ended OK" / "Ended Not OK" -> "Succ" / "Abend"         (RMJ "Completed" / "Error")

API reference:
  https://www.ibm.com/docs/en/workload-automation
  (exact REST paths vary between the distributed engine `/twsd/v1` and the
  z/OS engine `/twsz/v1`, and across product versions — see README).
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

class IWSJobSpec(dg.Model, dg.Resolvable):
    """An IWS Job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_name: str = Field(
        description="IWS Job name (as registered in the IWS Symphony / plan).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this IWS Job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    application: str = Field(
        default="",
        description="IWS Application (JobStream) — analog to Control-M Folder / RMJ Application.",
    )
    workstation: str = Field(
        default="",
        description="Target IWS Workstation — analog to Control-M Agent/Host / RMJ Queue.",
    )
    alias_name: str = Field(default="", description="Business tag / alias for the job.")
    run_as: str = Field(default="svc_dagster", description="OS user IWS runs the job as.")


class IWSSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by an IWS job that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="iws_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the IWS job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_iws(context: AssetExecutionContext, job: IWSJobSpec, endpoint: str) -> dict:
    """Simulate the IWS REST API lifecycle to stdout — no external deps."""
    job_id = f"IWS-{uuid.uuid4().hex[:8].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "workstation": job.workstation,
        "name": job.job_name,
        "application": job.application,
        "priority": 50,
        "aliasName": job.alias_name,
        "variables": {},
    }
    fake_basic = base64.b64encode(b"svc_dagster:******").decode()

    context.log.info(f"[AUTH]    Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[SUBMIT]  POST {endpoint}/plan/current/jobstream - {job.application}/{job.job_name}")
    context.log.info(f"  Payload: {json.dumps(submit_payload, indent=2)}")
    context.log.info(f"  Response: 201 Created - jobId: {job_id}")

    for state in ["Waiting", "Ready", "Running", "Running", "Succ"]:
        context.log.info(f"[POLL]    GET {endpoint}/plan/current/job/{job_id} -> status={state}")

    context.log.info(f"[STDLIST] GET {endpoint}/plan/current/job/{job_id}/stdlist -> 487 lines")
    context.log.info(f"[DONE]    {job.job_name} -> Succ (jobId: {job_id}, workstation: {job.workstation})")

    return {"job_id": job_id, "status": "Succ", "scheduled_time": scheduled, "target": "iws"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _iws_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_iws(
    context: AssetExecutionContext,
    job: IWSJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    stdlist_retrieval: bool,
) -> dict:
    """Real IWS REST API lifecycle.

    REST paths vary between the distributed engine (`/twsd/v1`) and the
    z/OS engine (`/twsz/v1`), and across product versions. The paths
    below target the modern JSON REST surface for the distributed engine.
    If your instance uses a different prefix, override `endpoint`.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    headers = _iws_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "workstation": job.workstation,
        "name": job.job_name,
        "application": job.application,
        "priority": 50,
        "aliasName": job.alias_name,
        "variables": {},
    }
    context.log.info(f"[SUBMIT] POST {endpoint}/plan/current/jobstream - {job.job_name}")
    resp = requests.post(
        f"{endpoint}/plan/current/jobstream",
        json=submit_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    body = resp.json() if resp.content else {}
    job_id = body.get("jobId") or body.get("id", "unknown")
    context.log.info(f"  jobId: {job_id}")

    start = time.time()
    terminal_states = {"Succ", "Abend", "Cancelled"}
    status = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/plan/current/job/{job_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "UNKNOWN")
        context.log.info(f"[POLL]   {job.job_name} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_name} timed out after {poll_timeout}s")

    if status != "Succ":
        raise RuntimeError(f"{job.job_name} finished: {status}")

    output_lines = 0
    if stdlist_retrieval:
        try:
            out_resp = requests.get(
                f"{endpoint}/plan/current/job/{job_id}/stdlist",
                headers=headers, verify=False, timeout=30,
            )
            if out_resp.ok:
                output_lines = len(out_resp.text.split("\n"))
                context.log.info(f"[STDLIST] {output_lines} lines")
                for line in out_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"stdlist retrieval failed: {e}")

    return {"job_id": job_id, "status": status, "scheduled_time": scheduled, "output_lines": output_lines, "target": "iws"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class IWSIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """IBM Workload Scheduler (IWS / TWS) REST API integration.

    Each declared IWS Job becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    IWS-managed tables into Dagster's lineage graph. Operational tasks
    (rerun / hold / release / kill / reconcile) ship as Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no IWS endpoint. Flip to `false`
    and set `IWS_USER` / `IWS_PASSWORD` to hit a real IWS instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://iws.internal:31116/twsd/v1",
        description="IWS REST API base URL (used when demo_mode=false). Distributed engine uses /twsd/v1; z/OS engine uses /twsz/v1.",
    )
    jobs: List[IWSJobSpec] = Field(
        default_factory=list,
        description="IWS Jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[IWSSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by IWS that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="iws_integration",
        description="Dagster asset group for the job assets.",
    )

    iws_user_env: str = Field(
        default="IWS_USER",
        description="Env var holding the IWS REST username.",
    )
    iws_password_env: str = Field(
        default="IWS_PASSWORD",
        description="Env var holding the IWS REST password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between IWS job status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    stdlist_retrieval: bool = Field(default=True, description="Retrieve job stdlist on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the IWS vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.iws_user_env
        password_env = self.iws_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        stdlist_retrieval = self.stdlist_retrieval

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
                    kinds={"python", "iws"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"IWS Job: {_job.job_name}. "
                        f"Submits to {_job.application} (workstation: {_job.workstation or 'default'}), "
                        f"polls for completion, retrieves stdlist."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "iws",
                        "integration_pattern": "dagster-orchestrates-iws",
                        "scheduler_owner": "IBM Workload Scheduler",
                        "iws_application": _job.application,
                        "iws_workstation": _job.workstation,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_iws(context, _job, _endpoint)
                    else:
                        result = _execute_iws(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, stdlist_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_job_id": result.get("job_id", "N/A"),
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
                    kinds={"iws", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by IWS: {_table.table_name}. "
                        f"Data is loaded by IWS jobs."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "iws_managed",
                        "table_name": _table.table_name,
                        "managed_by": "IBM Workload Scheduler",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: IBM Workload Scheduler (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown IWS job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="iws_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors IWS for job completions Dagster didn't trigger.",
        )
        def iws_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                job_id = f"ext-{uuid.uuid4().hex[:8]}"
                context.log.info(f"[DETECTED] External IWS job")
                context.log.info(f"  Triggered by: IWS plan / schedule")
                context.log.info(f"  Status: Succ")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  jobId: {job_id}")
                context.log.info(f"  Dagster was NOT the orchestrator - recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "iws_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_job_id": dg.MetadataValue.text(job_id),
                        "platform": dg.MetadataValue.text("iws"),
                        "triggered_by": dg.MetadataValue.text("IWS schedule"),
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
                        headers = _iws_headers(user, pw)
                        resp = requests.get(
                            f"{_ep}/plan/current/job",
                            params={"status": "Succ", "limit": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for j in resp.json().get("jobs", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"iws_{(j.get('name') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(str(j.get("jobId", "unknown"))),
                                    "status": dg.MetadataValue.text(j.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"IWS monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="IWS-UNKNOWN")})
        def rerun_iws_job(context: dg.OpExecutionContext):
            """Rerun a failed IWS job. Config: {job_id: "IWS-ABC"}."""
            job_id = context.op_config.get("job_id", "IWS-UNKNOWN")
            if _demo:
                context.log.info(f"[RERUN]   POST {_ep}/plan/current/job/{job_id}/action/rerun")
                context.log.info(f"  Response: 200 OK - job resubmitted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _iws_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/plan/current/job/{job_id}/action/rerun",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RERUN]   Job {job_id} resubmitted")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="IWS-UNKNOWN")})
        def hold_iws_job(context: dg.OpExecutionContext):
            """Hold an IWS job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", "IWS-UNKNOWN")
            if _demo:
                context.log.info(f"[HOLD]    POST {_ep}/plan/current/job/{job_id}/action/hold")
                context.log.info(f"  Response: 200 OK - job held")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _iws_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/plan/current/job/{job_id}/action/hold",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="IWS-UNKNOWN")})
        def release_iws_job(context: dg.OpExecutionContext):
            """Release a held IWS job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", "IWS-UNKNOWN")
            if _demo:
                context.log.info(f"[RELEASE] POST {_ep}/plan/current/job/{job_id}/action/release")
                context.log.info(f"  Response: 200 OK - job released")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _iws_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/plan/current/job/{job_id}/action/release",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="IWS-UNKNOWN")})
        def kill_iws_job(context: dg.OpExecutionContext):
            """Kill a running IWS job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", "IWS-UNKNOWN")
            if _demo:
                context.log.info(f"[KILL]    POST {_ep}/plan/current/job/{job_id}/action/kill")
                context.log.info(f"  Response: 200 OK - job terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _iws_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/plan/current/job/{job_id}/action/kill",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_iws_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with IWS' actual plan state."""
            if _demo:
                context.log.info(f"[RECON]   GET {_ep}/plan/current/job?status=Succ,Abend&limit=200")
                context.log.info(f"  IWS: 58 jobs - 49 Succ, 3 Abend, 4 Running, 2 Waiting")
                context.log.info(f"  Dagster: materializations for 47 of 49 'Succ' jobs")
                context.log.info(f"  DRIFT: 2 jobs completed in IWS but not in Dagster")
                context.log.info(f"  ALERT: 3 jobs in 'Abend' - manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _iws_headers(u or "", p or "")
                resp = _req.get(
                    f"{_ep}/plan/current/job",
                    params={"status": "Succ,Abend", "limit": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                jobs = resp.json().get("jobs", [])
                by_status: dict = {}
                for jb in jobs:
                    by_status.setdefault(jb.get("status", "?"), []).append(jb)
                context.log.info(f"[RECON]   IWS: {len(jobs)} jobs")
                for st, jbs in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(jbs)}")
                for jb in by_status.get("Abend", []):
                    context.log.warning(f"  ALERT: {jb.get('name')} - Abend")

        @dg.job(description="Rerun a failed IWS job.")
        def iws_rerun_job():
            rerun_iws_job()

        @dg.job(description="Hold an IWS job.")
        def iws_hold_job():
            hold_iws_job()

        @dg.job(description="Release a held IWS job.")
        def iws_release_job():
            release_iws_job()

        @dg.job(description="Kill a running IWS job.")
        def iws_kill_job():
            kill_iws_job()

        @dg.job(description="Reconcile Dagster state with IWS - detect drift.")
        def iws_reconciliation():
            reconcile_iws_state()

        recon_schedule = dg.ScheduleDefinition(
            name="iws_reconciliation_schedule",
            job=iws_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="iws_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for IWS-initiated pipeline triggers. In production, "
                "an IWS job calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled IWS job completes."
            ),
        )
        def iws_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND] IWS trigger detected (tick {tick})")
                context.log.info(f"  Source: IWS scheduled job completion")
                context.log.info(f"  Action: In production, IWS calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[iws_external_monitor, iws_inbound_trigger],
            jobs=[
                iws_rerun_job, iws_hold_job,
                iws_release_job, iws_kill_job,
                iws_reconciliation,
            ],
            schedules=[recon_schedule],
        )
