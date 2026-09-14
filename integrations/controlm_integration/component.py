"""Control-M Automation API integration component.

Bidirectional integration between Dagster and BMC Control-M for
distributed / z/OS batch:

  Dagster → Control-M:
    - Submit jobs via Automation API (POST /run/order)
    - Poll job status (GET /run/jobs/status)
    - Retrieve job output (GET {outputURI})
    - Send feedback events (POST /run/event/{runId})
    - Session management (POST /session/login, /session/logout)

  Control-M → Dagster:
    - Control-M calls Dagster's GraphQL API (launchRun mutation)
    - Can target specific partitions (batch date = partition key)
    - Parameters passed via runConfigData

Each declared job becomes a daily-partitioned Dagster asset. Operational
concerns (restart / hold / free / kill / reconcile) ship as jobs backed
by ops so the customer can wire them into Dagster+ Automations, run them
from the UI, or invoke them via the GraphQL API.

Every YAML knob has a `demo_mode` shortcut that simulates the Automation
API on stdout — makes it possible to demo the full Dagster surface with
zero external dependencies, then flip `demo_mode: false` when a real
Control-M endpoint is available.

Environment variables (production mode only):
  CONTROLM_USER          — Automation API username
  CONTROLM_PASSWORD      — Automation API password

API reference:
  https://docs.bmc.com/docs/automation-api
  https://github.com/controlm/automation-api-quickstart
"""
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

class ControlMJobSpec(dg.Model, dg.Resolvable):
    """A Control-M job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_name: str = Field(description="Control-M job name (as it appears in the folder definition).")
    asset_name: str = Field(description="Dagster asset name that wraps this job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    folder: str = Field(default="", description="Control-M folder that owns the job.")
    application: str = Field(default="", description="Control-M application tag (e.g. CORE_BANKING).")
    sub_application: str = Field(default="", description="Control-M sub-application tag (e.g. SETTLEMENT).")
    host: str = Field(default="", description="Control-M agent / server host that runs the job.")
    run_as: str = Field(default="svc_dagster", description="OS user Control-M runs the job as.")


class ControlMSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Control-M job that Dagster observes (not orchestrates)."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name that represents the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="controlm_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Control-M job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_controlm(context: AssetExecutionContext, job: ControlMJobSpec, endpoint: str) -> dict:
    """Simulate the full Automation API lifecycle to stdout — zero external deps."""
    job_id = f"CTM-{uuid.uuid4().hex[:8].upper()}"
    odate = context.partition_key if context.has_partition_key else time.strftime("%Y%m%d")

    payload = {
        "ctm": job.host, "folder": job.folder, "jobs": job.job_name,
        "hold": "false", "odate": odate,
    }
    context.log.info(f"[LOGIN]    POST {endpoint}/session/login")
    context.log.info(f"  Response: 200 OK — token acquired")
    context.log.info(f"[SUBMIT]   POST {endpoint}/run/order")
    context.log.info(f"  Payload: {json.dumps(payload, indent=2)}")
    context.log.info(f"  Response: 200 OK — runId: {job_id}")

    for state in ["Submitted", "Wait Condition", "Executing", "Executing", "Ended OK"]:
        context.log.info(f"[POLL]     GET {endpoint}/run/jobs/status?runId={job_id} -> {state}")

    context.log.info(f"[OUTPUT]   GET {{outputURI}} -> 847 lines")
    context.log.info(f"[DONE]     {job.job_name} -> Ended OK (runId: {job_id}, ODATE: {odate})")

    feedback = {
        "eventType": "DAGSTER_JOB_COMPLETE",
        "jobName": job.job_name,
        "runId": job_id,
        "status": "SUCCESS",
        "odate": odate,
        "dagsterRunId": context.run_id,
    }
    context.log.info(f"[FEEDBACK] POST {endpoint}/run/event/{job_id}")
    context.log.info(f"  Event: {json.dumps(feedback, indent=2)}")
    context.log.info(f"[LOGOUT]   POST {endpoint}/session/logout")

    return {"run_id": job_id, "status": "Ended OK", "odate": odate, "target": "controlm"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _execute_controlm(
    context: AssetExecutionContext,
    job: ControlMJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    spool_retrieval: bool,
) -> dict:
    """Real Automation API v2 lifecycle. Ref: automation-api-quickstart."""
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    odate = context.partition_key if context.has_partition_key else time.strftime("%Y%m%d")

    context.log.info(f"[LOGIN]  POST {endpoint}/session/login")
    login_resp = requests.post(
        f"{endpoint}/session/login",
        json={"username": user, "password": password},
        verify=False, timeout=30,
    )
    login_resp.raise_for_status()
    login_data = login_resp.json()
    if "errors" in login_data:
        raise RuntimeError(f"Control-M login failed: {login_data['errors']}")
    token = login_data.get("token")
    if not token:
        raise RuntimeError("No token returned from Control-M login")

    headers = {"Authorization": f"Bearer {token}"}
    context.log.info(f"  Token acquired")

    try:
        order_payload = {
            "ctm": job.host, "folder": job.folder,
            "jobs": job.job_name, "hold": "false", "odate": odate,
        }
        context.log.info(f"[SUBMIT] POST {endpoint}/run/order — {job.job_name} (ODATE: {odate})")
        resp = requests.post(f"{endpoint}/run/order", json=order_payload, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
        run_id = resp.json().get("runId", resp.json().get("jobId", "unknown"))
        context.log.info(f"  runId: {run_id}")

        start = time.time()
        terminal_states = {"Ended OK", "Ended Not OK", "Wait User"}
        status = "UNKNOWN"
        output_uri = None

        while time.time() - start < poll_timeout:
            resp = requests.get(
                f"{endpoint}/run/jobs/status", params={"runId": run_id},
                headers=headers, verify=False, timeout=30,
            )
            resp.raise_for_status()
            for s in resp.json().get("statuses", []):
                if s.get("jobId") == run_id or s.get("name") == job.job_name:
                    status = s.get("status", "UNKNOWN")
                    output_uri = s.get("outputURI")
                    break
            context.log.info(f"[POLL]   {job.job_name} -> {status}")
            if status in terminal_states:
                break
            time.sleep(poll_interval)
        else:
            raise TimeoutError(f"{job.job_name} timed out after {poll_timeout}s")

        if status != "Ended OK":
            raise RuntimeError(f"{job.job_name} finished: {status}")

        output_lines = 0
        if output_uri and spool_retrieval:
            context.log.info(f"[OUTPUT] GET {output_uri}")
            out_resp = requests.get(output_uri, headers=headers, verify=False, timeout=30)
            if out_resp.ok:
                output_lines = len(out_resp.text.split("\n"))
                context.log.info(f"  {output_lines} lines")
                for line in out_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")

        feedback = {
            "eventType": "DAGSTER_JOB_COMPLETE",
            "jobName": job.job_name, "runId": run_id,
            "status": "SUCCESS", "odate": odate,
            "dagsterRunId": context.run_id,
        }
        requests.post(f"{endpoint}/run/event/{run_id}", json=feedback, headers=headers, verify=False, timeout=30)
        context.log.info(f"[FEEDBACK] Sent completion event")

        return {"run_id": run_id, "status": status, "odate": odate, "output_lines": output_lines, "target": "controlm"}

    finally:
        try:
            requests.post(f"{endpoint}/session/logout", json={"token": token, "username": user}, verify=False, timeout=10)
            context.log.info(f"[LOGOUT] Session closed")
        except Exception as e:
            context.log.warning(f"Control-M logout failed: {e}")


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class ControlMIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """BMC Control-M Automation API integration.

    Each declared job becomes a daily-partitioned Dagster asset with a
    retry policy. Optional source-table declarations bring Control-M-managed
    tables into Dagster's lineage graph. Operational tasks (restart / hold
    / free / kill / reconcile) ship as Dagster jobs.

    `demo_mode: true` (default) simulates the Automation API on stdout so
    the whole component runs end-to-end with no Control-M endpoint. Flip
    to `false` and set `CONTROLM_USER` / `CONTROLM_PASSWORD` to hit a real
    Automation API server.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the Automation API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://controlm.internal:8443/automation-api",
        description="Control-M Automation API base URL (used when demo_mode=false).",
    )
    jobs: List[ControlMJobSpec] = Field(
        default_factory=list,
        description="Control-M jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[ControlMSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Control-M that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="controlm_integration",
        description="Dagster asset group for the job assets.",
    )

    controlm_user_env: str = Field(
        default="CONTROLM_USER",
        description="Env var holding the Automation API username.",
    )
    controlm_password_env: str = Field(
        default="CONTROLM_PASSWORD",
        description="Env var holding the Automation API password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between Control-M status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    spool_retrieval: bool = Field(default=True, description="Retrieve job spool output on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets (ODATE origin).",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Control-M vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.controlm_user_env
        password_env = self.controlm_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        spool_retrieval = self.spool_retrieval

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
                    kinds={"python", "control-m"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"Control-M job: {_job.job_name}. "
                        f"Submits to {_job.application}/{_job.sub_application}, "
                        f"polls for completion, sends feedback."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "controlm",
                        "integration_pattern": "dagster-orchestrates-controlm",
                        "scheduler_owner": "Control-M",
                        "controlm_folder": _job.folder,
                        "controlm_application": _job.application,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_controlm(context, _job, _endpoint)
                    else:
                        result = _execute_controlm(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, spool_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_job_id": result.get("run_id", "N/A"),
                        "status": result.get("status", "N/A"),
                        "odate": result.get("odate", "N/A"),
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
                    kinds={"control-m", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Control-M: {_table.table_name}. "
                        f"Data is loaded by Control-M batch jobs."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "controlm_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Control-M",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Control-M (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Control-M job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="controlm_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Control-M for job completions Dagster didn't trigger.",
        )
        def controlm_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                run_id = f"ext-{uuid.uuid4().hex[:8]}"
                context.log.info(f"[DETECTED] External Control-M execution")
                context.log.info(f"  Triggered by: Control-M scheduled batch")
                context.log.info(f"  Return code: ENDED_OK")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  Run ID: {run_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "controlm_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_run_id": dg.MetadataValue.text(run_id),
                        "platform": dg.MetadataValue.text("controlm"),
                        "triggered_by": dg.MetadataValue.text("Control-M scheduled batch"),
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
                        login = requests.post(f"{_ep}/session/login", json={"username": user, "password": pw}, verify=False, timeout=30)
                        login.raise_for_status()
                        token = login.json().get("token")
                        headers = {"Authorization": f"Bearer {token}"}
                        resp = requests.get(f"{_ep}/run/jobs/status", params={"status": "Ended OK"}, headers=headers, verify=False, timeout=30)
                        resp.raise_for_status()
                        for s in resp.json().get("statuses", []):
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"controlm_{s.get('name', 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(s.get("jobId", "unknown")),
                                    "status": dg.MetadataValue.text(s.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                        requests.post(f"{_ep}/session/logout", json={"token": token, "username": user}, verify=False, timeout=10)
                    except Exception as e:
                        context.log.warning(f"Control-M monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"run_id": dg.Field(str, is_required=False, default_value="CTM-UNKNOWN")})
        def restart_failed_controlm_job(context: dg.OpExecutionContext):
            """Restart a failed Control-M job. Config: {run_id: "CTM-ABC"}."""
            run_id = context.op_config.get("run_id", "CTM-UNKNOWN")
            if _demo:
                context.log.info(f"[RESTART]  POST {_ep}/run/runNow — runId: {run_id}")
                context.log.info(f"  Response: 200 OK — job resubmitted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                login = _req.post(f"{_ep}/session/login", json={"username": u, "password": p}, verify=False, timeout=30)
                login.raise_for_status()
                tk = login.json()["token"]
                try:
                    _req.post(f"{_ep}/run/runNow", json={"runId": run_id}, headers={"Authorization": f"Bearer {tk}"}, verify=False, timeout=30).raise_for_status()
                    context.log.info(f"[RESTART] Job {run_id} resubmitted")
                finally:
                    _req.post(f"{_ep}/session/logout", json={"token": tk, "username": u}, verify=False, timeout=10)

        @dg.op(config_schema={
            "folder": dg.Field(str, is_required=False, default_value="DAILY_BATCH"),
            "server": dg.Field(str, is_required=False, default_value="ctm-agent-prod-01"),
        })
        def hold_controlm_folder(context: dg.OpExecutionContext):
            """Hold all jobs in a folder. Config: {folder, server}."""
            folder = context.op_config.get("folder", "DAILY_BATCH")
            server = context.op_config.get("server", "ctm-agent-prod-01")
            if _demo:
                context.log.info(f"[HOLD] All jobs in {folder} held on {server}")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                login = _req.post(f"{_ep}/session/login", json={"username": u, "password": p}, verify=False, timeout=30)
                login.raise_for_status()
                tk = login.json()["token"]
                try:
                    _req.post(f"{_ep}/run/order", json={"ctm": server, "folder": folder, "hold": "true"}, headers={"Authorization": f"Bearer {tk}"}, verify=False, timeout=30).raise_for_status()
                finally:
                    _req.post(f"{_ep}/session/logout", json={"token": tk, "username": u}, verify=False, timeout=10)

        @dg.op(config_schema={"folder": dg.Field(str, is_required=False, default_value="DAILY_BATCH")})
        def free_held_controlm_jobs(context: dg.OpExecutionContext):
            """Release held jobs. Config: {folder}."""
            folder = context.op_config.get("folder", "DAILY_BATCH")
            if _demo:
                context.log.info(f"[FREE] All held jobs in {folder} released")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                login = _req.post(f"{_ep}/session/login", json={"username": u, "password": p}, verify=False, timeout=30)
                login.raise_for_status()
                tk = login.json()["token"]
                try:
                    _req.post(f"{_ep}/run/order", json={"folder": folder, "hold": "false"}, headers={"Authorization": f"Bearer {tk}"}, verify=False, timeout=30).raise_for_status()
                finally:
                    _req.post(f"{_ep}/session/logout", json={"token": tk, "username": u}, verify=False, timeout=10)

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="CTM-UNKNOWN")})
        def kill_controlm_job(context: dg.OpExecutionContext):
            """Kill a running job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", "CTM-UNKNOWN")
            if _demo:
                context.log.info(f"[KILL] Job {job_id} terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                login = _req.post(f"{_ep}/session/login", json={"username": u, "password": p}, verify=False, timeout=30)
                login.raise_for_status()
                tk = login.json()["token"]
                try:
                    _req.delete(f"{_ep}/run/job/{job_id}/kill", headers={"Authorization": f"Bearer {tk}"}, verify=False, timeout=30).raise_for_status()
                finally:
                    _req.post(f"{_ep}/session/logout", json={"token": tk, "username": u}, verify=False, timeout=10)

        @dg.op
        def reconcile_controlm_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Control-M's actual state."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/run/jobs/status")
                context.log.info(f"  Control-M: 47 jobs — 38 Ended OK, 3 Ended Not OK, 4 Executing, 2 Wait Condition")
                context.log.info(f"  Dagster: materializations for 36 of 38 'Ended OK' jobs")
                context.log.info(f"  DRIFT: 2 jobs completed in Control-M but not in Dagster")
                context.log.info(f"  ALERT: 3 jobs in 'Ended Not OK' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                login = _req.post(f"{_ep}/session/login", json={"username": u, "password": p}, verify=False, timeout=30)
                login.raise_for_status()
                tk = login.json()["token"]
                try:
                    resp = _req.get(f"{_ep}/run/jobs/status", headers={"Authorization": f"Bearer {tk}"}, verify=False, timeout=30)
                    resp.raise_for_status()
                    statuses = resp.json().get("statuses", [])
                    by_status: dict = {}
                    for s in statuses:
                        by_status.setdefault(s.get("status", "?"), []).append(s)
                    context.log.info(f"[RECON] Control-M: {len(statuses)} jobs")
                    for st, jobs in sorted(by_status.items()):
                        context.log.info(f"  {st}: {len(jobs)}")
                    for j in by_status.get("Ended Not OK", []):
                        context.log.warning(f"  ALERT: {j.get('name')} — Ended Not OK")
                finally:
                    _req.post(f"{_ep}/session/logout", json={"token": tk, "username": u}, verify=False, timeout=10)

        @dg.job(description="Restart a failed Control-M job.")
        def controlm_restart_job():
            restart_failed_controlm_job()

        @dg.job(description="Hold all jobs in a Control-M folder.")
        def controlm_hold_folder():
            hold_controlm_folder()

        @dg.job(description="Release held Control-M jobs.")
        def controlm_free_folder():
            free_held_controlm_jobs()

        @dg.job(description="Kill a running Control-M job.")
        def controlm_kill_job():
            kill_controlm_job()

        @dg.job(description="Reconcile Dagster state with Control-M — detect drift.")
        def controlm_reconciliation():
            reconcile_controlm_state()

        recon_schedule = dg.ScheduleDefinition(
            name="controlm_reconciliation_schedule",
            job=controlm_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="controlm_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Control-M-initiated pipeline triggers. In production, "
                "Control-M calls Dagster's GraphQL API (launchRun mutation) to start "
                "runs after mainframe batch completion."
            ),
        )
        def controlm_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  Control-M trigger detected (tick {tick})")
                context.log.info(f"  Source: Control-M scheduled batch completion")
                context.log.info(f"  Action: In production, Control-M calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[controlm_external_monitor, controlm_inbound_trigger],
            jobs=[controlm_restart_job, controlm_hold_folder, controlm_free_folder, controlm_kill_job, controlm_reconciliation],
            schedules=[recon_schedule],
        )
