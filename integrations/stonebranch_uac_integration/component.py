"""Stonebranch Universal Automation Center (UAC) integration component.

Bidirectional integration between Dagster and Stonebranch UAC (Universal
Controller / Universal Agent) via the UAC REST API:

  Dagster -> Stonebranch UAC:
    - Launch a Task (POST /resources/task/ops-task-launch?taskname=...)
    - Poll the resulting Task Instance status (GET /resources/taskinstance/{sysId})
    - Retrieve task instance output (GET /resources/taskinstance/{sysId}/output)
    - Cancel / hold / release / rerun ops for operational control
    - HTTP Basic Auth (username:password in Authorization header)

  Stonebranch UAC -> Dagster:
    - A UAC Task can call Dagster's GraphQL API (launchRun mutation)
    - Parameters passed via runConfigData; partition selected via scheduled date

Each declared Task becomes a daily-partitioned Dagster asset. Operational
tasks (rerun / hold / release / cancel / reconcile) ship as Dagster jobs
backed by ops so the customer can wire them into the UI or Dagster+
Automations.

`demo_mode: true` (default) simulates the UAC REST API on stdout — the
whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  STONEBRANCH_USER      — REST API username (default demo image: ops.admin)
  STONEBRANCH_PASSWORD  — REST API password (default demo image: admin)

Terminology map — for teams migrating from Control-M or RunMyJobs:
  Control-M Job / RMJ JobDefinition   -> Stonebranch Task
  Control-M Folder / RMJ Application  -> Stonebranch Workflow
  Control-M Agent/Host / RMJ Queue    -> Stonebranch Universal Agent
  Control-M runId / RMJ processId     -> Stonebranch Task Instance (sysId)
  "Ended OK" / RMJ "Completed"        -> Stonebranch "Success"

API reference:
  https://docs.stonebranch.com/
  (exact REST paths vary by UAC version; verify against your instance
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

class StonebranchTaskSpec(dg.Model, dg.Resolvable):
    """A Stonebranch UAC Task wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    task_name: str = Field(
        description="Stonebranch UAC Task name (as registered in Universal Controller).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Task.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    workflow: str = Field(default="", description="Stonebranch Workflow (analog to Control-M Folder / RMJ Application).")
    application: str = Field(default="", description="Business tag for the task (e.g. CORE_BANKING).")
    agent: str = Field(default="", description="Stonebranch Universal Agent that runs the task (analog to Control-M Agent / RMJ Queue).")
    run_as: str = Field(default="svc_dagster", description="OS user Stonebranch runs the task as.")


class StonebranchSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Stonebranch UAC Task that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="stonebranch_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Stonebranch task that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_stonebranch(context: AssetExecutionContext, job: StonebranchTaskSpec, endpoint: str) -> dict:
    """Simulate the UAC REST API lifecycle to stdout — no external deps."""
    sys_id = uuid.uuid4().hex[:32]
    task_instance_id = f"UAC-{uuid.uuid4().hex[:8].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    launch_payload = {
        "taskname": job.task_name,
        "workflow": job.workflow,
        "agent": job.agent,
        "scheduledTime": scheduled,
        "variables": {},
    }
    fake_basic = base64.b64encode(b"ops.admin:******").decode()

    context.log.info(f"[AUTH]    Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[LAUNCH]  POST {endpoint}/resources/task/ops-task-launch?taskname={job.task_name}")
    context.log.info(f"  Payload: {json.dumps(launch_payload, indent=2)}")
    context.log.info(f"  Response: 200 OK — taskInstanceId: {task_instance_id}, sysId: {sys_id}")

    for state in ["Waiting", "Queued", "Running", "Running", "Success"]:
        context.log.info(f"[POLL]    GET {endpoint}/resources/taskinstance/{sys_id} -> status={state}")

    context.log.info(f"[OUTPUT]  GET {endpoint}/resources/taskinstance/{sys_id}/output -> 512 lines")
    context.log.info(f"[DONE]    {job.task_name} -> Success (sysId: {sys_id}, workflow: {job.workflow})")

    feedback = {
        "eventName": "DAGSTER_TASK_COMPLETE",
        "taskname": job.task_name,
        "sysId": sys_id,
        "taskInstanceId": task_instance_id,
        "status": "Success",
        "scheduledTime": scheduled,
        "dagsterRunId": context.run_id,
    }
    context.log.info(f"[EVENT]   POST {endpoint}/resources/taskinstance/{sys_id}/events")
    context.log.info(f"  Payload: {json.dumps(feedback, indent=2)}")

    return {
        "sys_id": sys_id,
        "task_instance_id": task_instance_id,
        "status": "Success",
        "scheduled_time": scheduled,
        "target": "stonebranch",
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _stonebranch_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_stonebranch(
    context: AssetExecutionContext,
    job: StonebranchTaskSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    output_retrieval: bool,
) -> dict:
    """Real Stonebranch UAC REST API lifecycle.

    REST paths vary across UAC versions; the paths below target the
    modern JSON REST surface (UAC 7.x+). Older UAC installs may still
    require XML — verify against your instance. If your UAC uses a
    different base path (e.g. `/uc/ws/`), override `endpoint` to
    include the full prefix.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    headers = _stonebranch_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    launch_payload = {
        "taskname": job.task_name,
        "workflow": job.workflow,
        "agent": job.agent,
        "scheduledTime": scheduled,
        "variables": {},
    }
    context.log.info(f"[LAUNCH]  POST {endpoint}/resources/task/ops-task-launch?taskname={job.task_name}")
    resp = requests.post(
        f"{endpoint}/resources/task/ops-task-launch",
        params={"taskname": job.task_name},
        json=launch_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    sys_id = body.get("sysId") or body.get("id", "unknown")
    task_instance_id = body.get("taskInstanceId", "unknown")
    context.log.info(f"  sysId: {sys_id}, taskInstanceId: {task_instance_id}")

    start = time.time()
    terminal_states = {"Success", "Failed", "Cancelled", "Skipped"}
    status = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/resources/taskinstance/{sys_id}",
            params={"sysId": sys_id},
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "UNKNOWN")
        context.log.info(f"[POLL]    {job.task_name} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.task_name} timed out after {poll_timeout}s")

    if status != "Success":
        raise RuntimeError(f"{job.task_name} finished: {status}")

    output_lines = 0
    if output_retrieval:
        try:
            out_resp = requests.get(
                f"{endpoint}/resources/taskinstance/{sys_id}/output",
                headers=headers, verify=False, timeout=30,
            )
            if out_resp.ok:
                output_lines = len(out_resp.text.split("\n"))
                context.log.info(f"[OUTPUT]  {output_lines} lines")
                for line in out_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"output retrieval failed: {e}")

    try:
        feedback = {
            "eventName": "DAGSTER_TASK_COMPLETE",
            "taskname": job.task_name, "sysId": sys_id,
            "taskInstanceId": task_instance_id,
            "status": "Success", "scheduledTime": scheduled,
            "dagsterRunId": context.run_id,
        }
        requests.post(
            f"{endpoint}/resources/taskinstance/{sys_id}/events",
            json=feedback, headers=headers, verify=False, timeout=30,
        )
        context.log.info(f"[EVENT]   Sent completion event")
    except Exception as e:
        context.log.warning(f"event post failed: {e}")

    return {
        "sys_id": sys_id,
        "task_instance_id": task_instance_id,
        "status": status,
        "scheduled_time": scheduled,
        "output_lines": output_lines,
        "target": "stonebranch",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class StonebranchUACIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Stonebranch Universal Automation Center (UAC) REST API integration.

    Each declared Task becomes a daily-partitioned Dagster asset with a
    retry policy. Optional source-table declarations bring Stonebranch-
    managed tables into Dagster's lineage graph. Operational tasks
    (rerun / hold / release / cancel / reconcile) ship as Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no UAC endpoint. Flip to `false`
    and set `STONEBRANCH_USER` / `STONEBRANCH_PASSWORD` to hit a real
    Stonebranch UAC instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="http://localhost:8080/uc",
        description="Stonebranch UAC REST API base URL (used when demo_mode=false).",
    )
    jobs: List[StonebranchTaskSpec] = Field(
        default_factory=list,
        description="Stonebranch UAC Tasks to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[StonebranchSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Stonebranch tasks that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before tasks run.",
    )
    group_name: str = Field(
        default="stonebranch_uac_integration",
        description="Dagster asset group for the task assets.",
    )

    stonebranch_user_env: str = Field(
        default="STONEBRANCH_USER",
        description="Env var holding the Stonebranch UAC REST username.",
    )
    stonebranch_password_env: str = Field(
        default="STONEBRANCH_PASSWORD",
        description="Env var holding the Stonebranch UAC REST password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between Stonebranch task instance status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    output_retrieval: bool = Field(default=True, description="Retrieve task instance output on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Stonebranch vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.stonebranch_user_env
        password_env = self.stonebranch_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        output_retrieval = self.output_retrieval

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
                    kinds={"python", "stonebranch"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"Stonebranch UAC Task: {_job.task_name}. "
                        f"Launches on {_job.workflow or 'default workflow'} (agent: {_job.agent or 'default'}), "
                        f"polls for completion, emits event."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "stonebranch",
                        "integration_pattern": "dagster-orchestrates-stonebranch",
                        "scheduler_owner": "Stonebranch UAC",
                        "uac_workflow": _job.workflow,
                        "uac_agent": _job.agent,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_stonebranch(context, _job, _endpoint)
                    else:
                        result = _execute_stonebranch(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, output_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "sys_id": result.get("sys_id", "N/A"),
                        "task_instance_id": result.get("task_instance_id", "N/A"),
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
                    kinds={"stonebranch", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Stonebranch UAC: {_table.table_name}. "
                        f"Data is loaded by Stonebranch tasks."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "stonebranch_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Stonebranch UAC",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]   {_table.table_name}")
                    context.log.info(f"  Managed by: Stonebranch UAC (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Stonebranch task'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="stonebranch_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Stonebranch UAC for task instance completions Dagster didn't trigger.",
        )
        def stonebranch_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                sys_id = uuid.uuid4().hex[:32]
                context.log.info(f"[DETECTED] External Stonebranch UAC task instance")
                context.log.info(f"  Triggered by: UAC schedule")
                context.log.info(f"  Status: Success")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  sysId: {sys_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "stonebranch_task"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "sys_id": dg.MetadataValue.text(sys_id),
                        "platform": dg.MetadataValue.text("stonebranch"),
                        "triggered_by": dg.MetadataValue.text("UAC schedule"),
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
                        headers = _stonebranch_headers(user, pw)
                        resp = requests.get(
                            f"{_ep}/resources/taskinstance/list",
                            params={"status": "Success", "lastRunAfter": "1h"},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        instances = resp.json() if isinstance(resp.json(), list) else resp.json().get("taskInstances", [])
                        for ti in instances[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"sb_{(ti.get('taskname') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "sys_id": dg.MetadataValue.text(str(ti.get("sysId", "unknown"))),
                                    "status": dg.MetadataValue.text(ti.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"Stonebranch monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"sys_id": dg.Field(str, is_required=False, default_value="UNKNOWN-SYSID")})
        def rerun_stonebranch_task_instance(context: dg.OpExecutionContext):
            """Rerun a Stonebranch UAC task instance. Config: {sys_id: "..."}."""
            sys_id = context.op_config.get("sys_id", "UNKNOWN-SYSID")
            if _demo:
                context.log.info(f"[RERUN]   POST {_ep}/resources/taskinstance/{sys_id}/ops-task-rerun")
                context.log.info(f"  Response: 200 OK — task instance requeued")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _stonebranch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/resources/taskinstance/{sys_id}/ops-task-rerun",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RERUN]   Task instance {sys_id} requeued")

        @dg.op(config_schema={"sys_id": dg.Field(str, is_required=False, default_value="UNKNOWN-SYSID")})
        def hold_stonebranch_task_instance(context: dg.OpExecutionContext):
            """Hold a Stonebranch UAC task instance. Config: {sys_id}."""
            sys_id = context.op_config.get("sys_id", "UNKNOWN-SYSID")
            if _demo:
                context.log.info(f"[HOLD]    POST {_ep}/resources/taskinstance/{sys_id}/ops-task-hold")
                context.log.info(f"  Task instance {sys_id} held")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _stonebranch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/resources/taskinstance/{sys_id}/ops-task-hold",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"sys_id": dg.Field(str, is_required=False, default_value="UNKNOWN-SYSID")})
        def release_stonebranch_task_instance(context: dg.OpExecutionContext):
            """Release a held Stonebranch UAC task instance. Config: {sys_id}."""
            sys_id = context.op_config.get("sys_id", "UNKNOWN-SYSID")
            if _demo:
                context.log.info(f"[RELEASE] POST {_ep}/resources/taskinstance/{sys_id}/ops-task-release")
                context.log.info(f"  Task instance {sys_id} released")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _stonebranch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/resources/taskinstance/{sys_id}/ops-task-release",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"sys_id": dg.Field(str, is_required=False, default_value="UNKNOWN-SYSID")})
        def cancel_stonebranch_task_instance(context: dg.OpExecutionContext):
            """Cancel a running Stonebranch UAC task instance. Config: {sys_id}."""
            sys_id = context.op_config.get("sys_id", "UNKNOWN-SYSID")
            if _demo:
                context.log.info(f"[CANCEL]  POST {_ep}/resources/taskinstance/{sys_id}/ops-task-cancel")
                context.log.info(f"  Task instance {sys_id} cancelled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _stonebranch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/resources/taskinstance/{sys_id}/ops-task-cancel",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_stonebranch_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Stonebranch UAC's actual state."""
            if _demo:
                context.log.info(f"[RECON]   GET {_ep}/resources/taskinstance/list?status=Success,Failed&lastRunAfter=1h")
                context.log.info(f"  Stonebranch UAC: 48 task instances — 41 Success, 3 Failed, 3 Running, 1 Queued")
                context.log.info(f"  Dagster: materializations for 39 of 41 'Success' task instances")
                context.log.info(f"  DRIFT: 2 task instances succeeded in UAC but not in Dagster")
                context.log.info(f"  ALERT: 3 task instances in 'Failed' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _stonebranch_headers(u or "", p or "")
                resp = _req.get(
                    f"{_ep}/resources/taskinstance/list",
                    params={"status": "Success,Failed", "lastRunAfter": "1h"},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()
                instances = body if isinstance(body, list) else body.get("taskInstances", [])
                by_status: dict = {}
                for ti in instances:
                    by_status.setdefault(ti.get("status", "?"), []).append(ti)
                context.log.info(f"[RECON]   Stonebranch UAC: {len(instances)} task instances")
                for st, tis in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(tis)}")
                for ti in by_status.get("Failed", []):
                    context.log.warning(f"  ALERT: {ti.get('taskname')} — Failed")

        @dg.job(description="Rerun a Stonebranch UAC task instance.")
        def stonebranch_rerun_task_instance():
            rerun_stonebranch_task_instance()

        @dg.job(description="Hold a Stonebranch UAC task instance.")
        def stonebranch_hold_task_instance():
            hold_stonebranch_task_instance()

        @dg.job(description="Release a held Stonebranch UAC task instance.")
        def stonebranch_release_task_instance():
            release_stonebranch_task_instance()

        @dg.job(description="Cancel a running Stonebranch UAC task instance.")
        def stonebranch_cancel_task_instance():
            cancel_stonebranch_task_instance()

        @dg.job(description="Reconcile Dagster state with Stonebranch UAC — detect drift.")
        def stonebranch_reconciliation():
            reconcile_stonebranch_state()

        recon_schedule = dg.ScheduleDefinition(
            name="stonebranch_reconciliation_schedule",
            job=stonebranch_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="stonebranch_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Stonebranch UAC-initiated pipeline triggers. In production, "
                "a UAC Task calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled task instance completes."
            ),
        )
        def stonebranch_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND] Stonebranch UAC trigger detected (tick {tick})")
                context.log.info(f"  Source: UAC scheduled task instance completion")
                context.log.info(f"  Action: In production, UAC calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[stonebranch_external_monitor, stonebranch_inbound_trigger],
            jobs=[
                stonebranch_rerun_task_instance, stonebranch_hold_task_instance,
                stonebranch_release_task_instance, stonebranch_cancel_task_instance,
                stonebranch_reconciliation,
            ],
            schedules=[recon_schedule],
        )
