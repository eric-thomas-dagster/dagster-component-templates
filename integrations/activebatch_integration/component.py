"""ActiveBatch (Redwood) integration component.

Bidirectional integration between Dagster and ActiveBatch (now owned by
Redwood, historically Advanced Systems Concepts):

  Dagster -> ActiveBatch:
    - Trigger a Job (POST /Objects/{objectId}/Triggers)
    - Poll the resulting Instance status (GET /Instances/{instanceId})
    - Retrieve instance log (GET /Instances/{instanceId}/Log)
    - Abort / hold / release / restart ops for operational control
    - HTTP Basic Auth (username:password in Authorization header)

  ActiveBatch -> Dagster:
    - ActiveBatch job's post-step calls Dagster's GraphQL API (launchRun mutation)
    - Parameters passed via runConfigData; partition selected via date arguments

Each declared Job becomes a daily-partitioned Dagster asset. Operational
tasks (restart / hold / release / abort / reconcile) ship as Dagster jobs
backed by ops so the customer can wire them into the UI or Dagster+
Automations.

`demo_mode: true` (default) simulates the ActiveBatch REST API on stdout —
the whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  ACTIVEBATCH_USER      — REST API username
  ACTIVEBATCH_PASSWORD  — REST API password

Terminology map — for teams migrating from Control-M / RunMyJobs:
  Control-M Job          -> ActiveBatch Job
  Control-M Folder       -> ActiveBatch Plan
  Control-M Agent/Host   -> ActiveBatch Execution Queue
  Control-M ODATE        -> ActiveBatch scheduled date argument
  Control-M runId        -> ActiveBatch instanceId
  "Ended OK" / "Ended Not OK" -> "Succeeded" / "Failed"
  RunMyJobs Application  -> ActiveBatch Plan
  RunMyJobs Queue        -> ActiveBatch Execution Queue
  RunMyJobs processId    -> ActiveBatch instanceId
  RunMyJobs "Completed"  -> ActiveBatch "Succeeded"

API reference:
  https://www.advsyscon.com/en-us/activebatch
  (exact REST paths vary by ActiveBatch version; verify against your
  instance when flipping demo_mode to false — see README).
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

class ActiveBatchJobSpec(dg.Model, dg.Resolvable):
    """An ActiveBatch Job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    object_id: str = Field(
        description="ActiveBatch's numeric object ID for the Job (as shown in the AB console).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this ActiveBatch Job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    plan: str = Field(
        default="",
        description="ActiveBatch Plan (container, analog to Control-M Folder / RMJ Application).",
    )
    execution_queue: str = Field(
        default="",
        description="ActiveBatch Execution Queue (analog to Control-M Host / RMJ Queue).",
    )
    application: str = Field(default="", description="Business tag for the job (e.g. CORE_BANKING).")
    run_as: str = Field(default="svc_dagster", description="OS user ActiveBatch runs the job as.")


class ActiveBatchSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by an ActiveBatch job that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="activebatch_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the ActiveBatch job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_activebatch(context: AssetExecutionContext, job: ActiveBatchJobSpec, endpoint: str) -> dict:
    """Simulate the ActiveBatch REST API lifecycle to stdout — no external deps."""
    instance_id = f"AB-{uuid.uuid4().hex[:8].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    trigger_payload = {
        "arguments": {
            "scheduledDate": scheduled,
            "plan": job.plan,
            "executionQueue": job.execution_queue,
        },
        "priority": 5,
    }
    fake_basic = base64.b64encode(b"svc_dagster:******").decode()

    context.log.info(f"[AUTH]    Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[TRIGGER] POST {endpoint}/Objects/{job.object_id}/Triggers -> {job.object_id}")
    context.log.info(f"  Payload: {json.dumps(trigger_payload, indent=2)}")
    context.log.info(f"  Response: 201 Created — instanceId: {instance_id}")

    for state in ["Queued", "Queued", "Running", "Running", "Succeeded"]:
        context.log.info(f"[POLL]    GET {endpoint}/Instances/{instance_id} -> state={state}")

    context.log.info(f"[LOG]     GET {endpoint}/Instances/{instance_id}/Log -> 487 lines")
    context.log.info(f"[DONE]    {job.object_id} -> Succeeded (instanceId: {instance_id}, plan: {job.plan})")

    feedback = {
        "eventName": "DAGSTER_JOB_COMPLETE",
        "objectId": job.object_id,
        "instanceId": instance_id,
        "state": "Succeeded",
        "scheduledDate": scheduled,
        "dagsterRunId": context.run_id,
    }
    context.log.info(f"[EVENT]   POST {endpoint}/Instances/{instance_id}/Events")
    context.log.info(f"  Payload: {json.dumps(feedback, indent=2)}")

    return {"instance_id": instance_id, "state": "Succeeded", "scheduled_date": scheduled, "target": "activebatch"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _activebatch_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_activebatch(
    context: AssetExecutionContext,
    job: ActiveBatchJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    log_retrieval: bool,
) -> dict:
    """Real ActiveBatch REST API lifecycle.

    REST paths vary across ActiveBatch versions; the paths below target
    the modern JSON REST surface (v1). If your ActiveBatch instance uses
    a different prefix (e.g. /absvc/api/v2/…), override `endpoint` to
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

    headers = _activebatch_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    trigger_payload = {
        "arguments": {
            "scheduledDate": scheduled,
            "plan": job.plan,
            "executionQueue": job.execution_queue,
        },
        "priority": 5,
    }
    context.log.info(f"[TRIGGER] POST {endpoint}/Objects/{job.object_id}/Triggers — {job.object_id}")
    resp = requests.post(
        f"{endpoint}/Objects/{job.object_id}/Triggers",
        json=trigger_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    instance_id = body.get("instanceId") or body.get("id", "unknown")
    context.log.info(f"  instanceId: {instance_id}")

    start = time.time()
    terminal_states = {"Succeeded", "Failed", "Aborted", "Skipped"}
    state = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/Instances/{instance_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        state = resp.json().get("state", "UNKNOWN")
        context.log.info(f"[POLL]    {job.object_id} -> {state}")
        if state in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.object_id} timed out after {poll_timeout}s")

    if state != "Succeeded":
        raise RuntimeError(f"{job.object_id} finished: {state}")

    log_lines = 0
    if log_retrieval:
        try:
            log_resp = requests.get(
                f"{endpoint}/Instances/{instance_id}/Log",
                headers=headers, verify=False, timeout=30,
            )
            if log_resp.ok:
                log_lines = len(log_resp.text.split("\n"))
                context.log.info(f"[LOG]     {log_lines} lines")
                for line in log_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"log retrieval failed: {e}")

    try:
        feedback = {
            "eventName": "DAGSTER_JOB_COMPLETE",
            "objectId": job.object_id, "instanceId": instance_id,
            "state": "Succeeded", "scheduledDate": scheduled,
            "dagsterRunId": context.run_id,
        }
        requests.post(
            f"{endpoint}/Instances/{instance_id}/Events",
            json=feedback, headers=headers, verify=False, timeout=30,
        )
        context.log.info(f"[EVENT]   Sent completion event")
    except Exception as e:
        context.log.warning(f"event post failed: {e}")

    return {"instance_id": instance_id, "state": state, "scheduled_date": scheduled, "log_lines": log_lines, "target": "activebatch"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class ActiveBatchIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """ActiveBatch (Redwood) REST API integration.

    Each declared Job becomes a daily-partitioned Dagster asset with a
    retry policy. Optional source-table declarations bring
    ActiveBatch-managed tables into Dagster's lineage graph. Operational
    tasks (restart / hold / release / abort / reconcile) ship as Dagster
    jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no ActiveBatch endpoint. Flip
    to `false` and set `ACTIVEBATCH_USER` / `ACTIVEBATCH_PASSWORD` to
    hit a real ActiveBatch instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="http://activebatch.internal/absvc/api/v1",
        description="ActiveBatch REST API base URL (used when demo_mode=false).",
    )
    jobs: List[ActiveBatchJobSpec] = Field(
        default_factory=list,
        description="ActiveBatch Jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[ActiveBatchSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by ActiveBatch that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="activebatch_integration",
        description="Dagster asset group for the job assets.",
    )

    activebatch_user_env: str = Field(
        default="ACTIVEBATCH_USER",
        description="Env var holding the ActiveBatch REST username.",
    )
    activebatch_password_env: str = Field(
        default="ACTIVEBATCH_PASSWORD",
        description="Env var holding the ActiveBatch REST password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between ActiveBatch instance status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal state before failing.")
    log_retrieval: bool = Field(default=True, description="Retrieve instance log on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the ActiveBatch vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.activebatch_user_env
        password_env = self.activebatch_password_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        log_retrieval = self.log_retrieval

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
                    kinds={"python", "activebatch"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"ActiveBatch Job: {_job.object_id}. "
                        f"Triggers in {_job.plan or 'default plan'} "
                        f"(execution queue: {_job.execution_queue or 'default'}), "
                        f"polls for completion, emits event."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "activebatch",
                        "integration_pattern": "dagster-orchestrates-activebatch",
                        "scheduler_owner": "ActiveBatch",
                        "ab_plan": _job.plan,
                        "ab_execution_queue": _job.execution_queue,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_activebatch(context, _job, _endpoint)
                    else:
                        result = _execute_activebatch(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, log_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_instance_id": result.get("instance_id", "N/A"),
                        "state": result.get("state", "N/A"),
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
                    kinds={"activebatch", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by ActiveBatch: {_table.table_name}. "
                        f"Data is loaded by ActiveBatch jobs."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "activebatch_managed",
                        "table_name": _table.table_name,
                        "managed_by": "ActiveBatch",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]   {_table.table_name}")
                    context.log.info(f"  Managed by: ActiveBatch (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown ActiveBatch job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="activebatch_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors ActiveBatch for instance completions Dagster didn't trigger.",
        )
        def activebatch_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                instance_id = f"ext-{uuid.uuid4().hex[:8]}"
                context.log.info(f"[DETECTED] External ActiveBatch instance")
                context.log.info(f"  Triggered by: ActiveBatch schedule")
                context.log.info(f"  State: Succeeded")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  instanceId: {instance_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "activebatch_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_instance_id": dg.MetadataValue.text(instance_id),
                        "platform": dg.MetadataValue.text("activebatch"),
                        "triggered_by": dg.MetadataValue.text("ActiveBatch schedule"),
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
                        headers = _activebatch_headers(user, pw)
                        resp = requests.get(
                            f"{_ep}/Instances",
                            params={"filter": "state:Succeeded", "recent": "1h", "limit": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for inst in resp.json().get("instances", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"ab_{str(inst.get('objectId') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_instance_id": dg.MetadataValue.text(str(inst.get("instanceId", "unknown"))),
                                    "state": dg.MetadataValue.text(inst.get("state", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"ActiveBatch monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"instance_id": dg.Field(str, is_required=False, default_value="AB-UNKNOWN")})
        def restart_activebatch_instance(context: dg.OpExecutionContext):
            """Restart an ActiveBatch instance. Config: {instance_id: "AB-ABC"}."""
            instance_id = context.op_config.get("instance_id", "AB-UNKNOWN")
            if _demo:
                context.log.info(f"[RESTART] POST {_ep}/Instances/{instance_id}/Restart")
                context.log.info(f"  Response: 200 OK — instance restarted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _activebatch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Instances/{instance_id}/Restart",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] Instance {instance_id} restarted")

        @dg.op(config_schema={"instance_id": dg.Field(str, is_required=False, default_value="AB-UNKNOWN")})
        def hold_activebatch_instance(context: dg.OpExecutionContext):
            """Hold an ActiveBatch instance. Config: {instance_id}."""
            instance_id = context.op_config.get("instance_id", "AB-UNKNOWN")
            if _demo:
                context.log.info(f"[HOLD]    POST {_ep}/Instances/{instance_id}/Hold")
                context.log.info(f"  Instance {instance_id} placed on hold")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _activebatch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Instances/{instance_id}/Hold",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"instance_id": dg.Field(str, is_required=False, default_value="AB-UNKNOWN")})
        def release_activebatch_instance(context: dg.OpExecutionContext):
            """Release a held ActiveBatch instance. Config: {instance_id}."""
            instance_id = context.op_config.get("instance_id", "AB-UNKNOWN")
            if _demo:
                context.log.info(f"[RELEASE] POST {_ep}/Instances/{instance_id}/Release")
                context.log.info(f"  Instance {instance_id} released from hold")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _activebatch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Instances/{instance_id}/Release",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"instance_id": dg.Field(str, is_required=False, default_value="AB-UNKNOWN")})
        def abort_activebatch_instance(context: dg.OpExecutionContext):
            """Abort a running ActiveBatch instance. Config: {instance_id}."""
            instance_id = context.op_config.get("instance_id", "AB-UNKNOWN")
            if _demo:
                context.log.info(f"[ABORT]   POST {_ep}/Instances/{instance_id}/Abort")
                context.log.info(f"  Instance {instance_id} aborted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _activebatch_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Instances/{instance_id}/Abort",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_activebatch_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with ActiveBatch's actual state."""
            if _demo:
                context.log.info(f"[RECON]   GET {_ep}/Instances?filter=state:Succeeded,Failed&recent=1h")
                context.log.info(f"  ActiveBatch: 47 instances — 40 Succeeded, 3 Failed, 3 Running, 1 Queued")
                context.log.info(f"  Dagster: materializations for 38 of 40 'Succeeded' instances")
                context.log.info(f"  DRIFT: 2 instances succeeded in ActiveBatch but not in Dagster")
                context.log.info(f"  ALERT: 3 instances in 'Failed' state — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _activebatch_headers(u or "", p or "")
                resp = _req.get(
                    f"{_ep}/Instances",
                    params={"filter": "state:Succeeded,Failed", "recent": "1h", "limit": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                instances = resp.json().get("instances", [])
                by_state: dict = {}
                for inst in instances:
                    by_state.setdefault(inst.get("state", "?"), []).append(inst)
                context.log.info(f"[RECON]   ActiveBatch: {len(instances)} instances")
                for st, insts in sorted(by_state.items()):
                    context.log.info(f"  {st}: {len(insts)}")
                for inst in by_state.get("Failed", []):
                    context.log.warning(f"  ALERT: {inst.get('objectId')} — Failed")

        @dg.job(description="Restart an ActiveBatch instance.")
        def activebatch_restart_instance():
            restart_activebatch_instance()

        @dg.job(description="Hold an ActiveBatch instance.")
        def activebatch_hold_instance():
            hold_activebatch_instance()

        @dg.job(description="Release a held ActiveBatch instance.")
        def activebatch_release_instance():
            release_activebatch_instance()

        @dg.job(description="Abort a running ActiveBatch instance.")
        def activebatch_abort_instance():
            abort_activebatch_instance()

        @dg.job(description="Reconcile Dagster state with ActiveBatch — detect drift.")
        def activebatch_reconciliation():
            reconcile_activebatch_state()

        recon_schedule = dg.ScheduleDefinition(
            name="activebatch_reconciliation_schedule",
            job=activebatch_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="activebatch_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for ActiveBatch-initiated pipeline triggers. In production, "
                "an ActiveBatch job calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled instance completes."
            ),
        )
        def activebatch_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND] ActiveBatch trigger detected (tick {tick})")
                context.log.info(f"  Source: ActiveBatch scheduled instance completion")
                context.log.info(f"  Action: In production, ActiveBatch calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[activebatch_external_monitor, activebatch_inbound_trigger],
            jobs=[
                activebatch_restart_instance, activebatch_hold_instance,
                activebatch_release_instance, activebatch_abort_instance,
                activebatch_reconciliation,
            ],
            schedules=[recon_schedule],
        )
