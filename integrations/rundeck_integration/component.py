"""Rundeck integration component.

Bidirectional integration between Dagster and Rundeck (OSS / Enterprise):

  Dagster -> Rundeck:
    - Run a Job (POST /api/{v}/job/{job_id}/executions)
    - Poll the resulting Execution status (GET /api/{v}/execution/{id})
    - Retrieve execution output (GET /api/{v}/execution/{id}/output)
    - Abort / disable / enable ops for operational control
    - Auth via X-Rundeck-Auth-Token header (user-issued API token)

  Rundeck -> Dagster:
    - Rundeck job step (script / http) can call Dagster's GraphQL API
      (launchRun mutation) to start a Dagster run after a Rundeck execution
      completes. Parameters flow through argString / option values.

Each declared Job becomes a daily-partitioned Dagster asset. Operational
tasks (restart / disable / enable / abort / reconcile) ship as Dagster
jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the Rundeck REST API on stdout —
the whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  RUNDECK_API_TOKEN — Rundeck API token (created in the Rundeck UI:
                      User Profile -> User API Tokens -> Generate New Token)

Terminology map — for teams migrating from Control-M / RunMyJobs:
  Control-M Job          -> Rundeck Job
  Control-M Folder       -> Rundeck Project
  Control-M Agent/Host   -> Rundeck Node / Node Filter
  Control-M ODATE        -> Rundeck argString option (e.g. -runDate <date>)
  Control-M runId        -> Rundeck executionId
  "Ended OK" / "Ended Not OK" -> "succeeded" / "failed"
  RunMyJobs JobDefinition -> Rundeck Job
  RunMyJobs Application  -> Rundeck Project
  RunMyJobs Queue        -> Rundeck Node Filter
  RunMyJobs processId    -> Rundeck executionId
  RunMyJobs "Completed"  -> Rundeck "succeeded"

API reference:
  https://docs.rundeck.com/docs/api/
  API version defaults to v47 (current 2026); older Rundeck installs use
  lower ints — set `api_version` accordingly.
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

class RundeckJobSpec(dg.Model, dg.Resolvable):
    """A Rundeck Job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(
        description="Rundeck Job UUID (as shown in the Rundeck UI job detail URL).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Rundeck Job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    project: str = Field(default="", description="Rundeck Project (analog to Control-M Folder / RMJ Application).")
    arg_string: str = Field(
        default="",
        description="Rundeck argString of option values (e.g. `-key1 value1 -key2 value2`).",
    )
    run_as: str = Field(default="svc_dagster", description="Rundeck asUser identity for the execution.")


class RundeckSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Rundeck execution that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="rundeck_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Rundeck job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_rundeck(
    context: AssetExecutionContext,
    job: RundeckJobSpec,
    endpoint: str,
    api_version: int,
) -> dict:
    """Simulate the REST API lifecycle to stdout — no external deps."""
    execution_id = str(uuid.uuid4().int % 10_000_000)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "argString": job.arg_string,
        "options": {},
        "asUser": job.run_as,
    }

    context.log.info(f"[AUTH]   X-Rundeck-Auth-Token: rdk-***... (bearer token)")
    context.log.info(f"[SUBMIT] POST {endpoint}/api/{api_version}/job/{job.job_id}/executions")
    context.log.info(f"  Payload: {json.dumps(submit_payload, indent=2)}")
    context.log.info(f"  Response: 200 OK — executionId: {execution_id}, project: {job.project}")

    for state in ["running", "running", "running", "running", "succeeded"]:
        context.log.info(f"[POLL]   GET {endpoint}/api/{api_version}/execution/{execution_id} -> status={state}")

    context.log.info(f"[OUTPUT] GET {endpoint}/api/{api_version}/execution/{execution_id}/output -> 47 entries")
    context.log.info(f"[DONE]   {job.job_id} -> succeeded (executionId: {execution_id}, project: {job.project})")

    return {
        "execution_id": execution_id,
        "status": "succeeded",
        "scheduled_time": scheduled,
        "target": "rundeck",
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _rundeck_headers(token: str) -> dict:
    return {
        "X-Rundeck-Auth-Token": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_rundeck(
    context: AssetExecutionContext,
    job: RundeckJobSpec,
    endpoint: str,
    api_version: int,
    token_env: str,
    poll_interval: int,
    poll_timeout: int,
    output_retrieval: bool,
) -> dict:
    """Real Rundeck REST API lifecycle."""
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")

    headers = _rundeck_headers(token)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "argString": job.arg_string,
        "options": {},
        "asUser": job.run_as,
    }
    context.log.info(f"[SUBMIT] POST {endpoint}/api/{api_version}/job/{job.job_id}/executions")
    resp = requests.post(
        f"{endpoint}/api/{api_version}/job/{job.job_id}/executions",
        json=submit_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    execution_id = str(body.get("id") or body.get("executionId", "unknown"))
    context.log.info(f"  executionId: {execution_id}, project: {body.get('project', job.project)}")

    start = time.time()
    terminal_states = {"succeeded", "failed", "aborted", "timedout", "missed"}
    status = "unknown"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/api/{api_version}/execution/{execution_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown")
        context.log.info(f"[POLL]   {job.job_id} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_id} timed out after {poll_timeout}s")

    if status != "succeeded":
        raise RuntimeError(f"{job.job_id} finished: {status}")

    output_entries = 0
    if output_retrieval:
        try:
            out_resp = requests.get(
                f"{endpoint}/api/{api_version}/execution/{execution_id}/output",
                headers=headers, verify=False, timeout=30,
            )
            if out_resp.ok:
                entries = out_resp.json().get("entries", [])
                output_entries = len(entries)
                context.log.info(f"[OUTPUT] {output_entries} entries")
                for entry in entries[:20]:
                    context.log.info(f"    [{entry.get('level', '?')}] {entry.get('log', '')}")
        except Exception as e:
            context.log.warning(f"output retrieval failed: {e}")

    return {
        "execution_id": execution_id,
        "status": status,
        "scheduled_time": scheduled,
        "output_entries": output_entries,
        "target": "rundeck",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class RundeckIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Rundeck REST API integration.

    Each declared Rundeck Job becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    Rundeck-managed tables into Dagster's lineage graph. Operational
    tasks (restart / disable / enable / abort / reconcile) ship as
    Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no Rundeck endpoint. Flip to
    `false` and set `RUNDECK_API_TOKEN` to hit a real Rundeck instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="http://localhost:4440",
        description="Rundeck base URL (used when demo_mode=false).",
    )
    api_version: int = Field(
        default=47,
        description="Rundeck API version (v47 current as of 2026; older installs use lower ints).",
    )
    jobs: List[RundeckJobSpec] = Field(
        default_factory=list,
        description="Rundeck Jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[RundeckSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Rundeck executions that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="rundeck_integration",
        description="Dagster asset group for the job assets.",
    )

    rundeck_token_env: str = Field(
        default="RUNDECK_API_TOKEN",
        description="Env var holding the Rundeck API token.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between Rundeck execution status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    output_retrieval: bool = Field(default=True, description="Retrieve execution output entries on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Rundeck vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        api_version = self.api_version
        group = self.group_name
        token_env = self.rundeck_token_env
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
                _api_version=api_version,
                _demo=demo_mode,
                _group=group,
                _upstream=resolved_upstream,
            ):
                @dg.asset(
                    name=_job.asset_name,
                    kinds={"python", "rundeck"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"Rundeck Job: {_job.job_id}. "
                        f"Runs in project {_job.project or '(unspecified)'}, "
                        f"polls for completion, retrieves output entries."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "rundeck",
                        "integration_pattern": "dagster-orchestrates-rundeck",
                        "scheduler_owner": "Rundeck",
                        "rundeck_project": _job.project,
                        "rundeck_arg_string": _job.arg_string,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_rundeck(context, _job, _endpoint, _api_version)
                    else:
                        result = _execute_rundeck(
                            context, _job, _endpoint, _api_version,
                            token_env,
                            poll_interval, poll_timeout, output_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_execution_id": result.get("execution_id", "N/A"),
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
                    kinds={"rundeck", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Rundeck: {_table.table_name}. "
                        f"Data is loaded by Rundeck executions."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "rundeck_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Rundeck",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Rundeck (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Rundeck execution'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint
        _av = api_version

        @dg.sensor(
            name="rundeck_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Rundeck for execution completions Dagster didn't trigger.",
        )
        def rundeck_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                execution_id = str(uuid.uuid4().int % 10_000_000)
                context.log.info(f"[DETECTED] External Rundeck execution")
                context.log.info(f"  Triggered by: Rundeck schedule")
                context.log.info(f"  Status: succeeded")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  executionId: {execution_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "rundeck_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_execution_id": dg.MetadataValue.text(execution_id),
                        "platform": dg.MetadataValue.text("rundeck"),
                        "triggered_by": dg.MetadataValue.text("Rundeck schedule"),
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
                token = os.environ.get(token_env)
                if token:
                    try:
                        headers = _rundeck_headers(token)
                        project = self.jobs[0].project if self.jobs else "default"
                        resp = requests.get(
                            f"{_ep}/api/{_av}/project/{project}/executions",
                            params={"status": "succeeded", "max": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for ex in resp.json().get("executions", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"rundeck_{str(ex.get('job', {}).get('name', 'unknown')).lower().replace(' ', '_')}"),
                                metadata={
                                    "external_execution_id": dg.MetadataValue.text(str(ex.get("id", "unknown"))),
                                    "status": dg.MetadataValue.text(ex.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"Rundeck monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def restart_failed_rundeck_execution(context: dg.OpExecutionContext):
            """Re-run a Rundeck job by ID (Rundeck has no per-execution rerun — you re-fire the job).

            Config: {job_id: "<uuid>"}.
            """
            job_id = context.op_config.get("job_id", "00000000-0000-0000-0000-000000000000")
            if _demo:
                context.log.info(f"[RESTART] POST {_ep}/api/{_av}/job/{job_id}/executions")
                context.log.info(f"  Response: 200 OK — new execution launched")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                tk = os.environ.get(token_env, "")
                headers = _rundeck_headers(tk)
                _req.post(
                    f"{_ep}/api/{_av}/job/{job_id}/executions",
                    json={}, headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] Job {job_id} re-fired")

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def disable_rundeck_job(context: dg.OpExecutionContext):
            """Disable execution of a Rundeck job (analog to Control-M hold, per-job).

            Config: {job_id: "<uuid>"}.
            """
            job_id = context.op_config.get("job_id", "00000000-0000-0000-0000-000000000000")
            if _demo:
                context.log.info(f"[DISABLE] POST {_ep}/api/{_av}/job/{job_id}/execution/disable")
                context.log.info(f"  Response: 200 OK — job disabled (no new executions)")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                tk = os.environ.get(token_env, "")
                headers = _rundeck_headers(tk)
                _req.post(
                    f"{_ep}/api/{_av}/job/{job_id}/execution/disable",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def enable_rundeck_job(context: dg.OpExecutionContext):
            """Enable execution of a previously-disabled Rundeck job.

            Config: {job_id: "<uuid>"}.
            """
            job_id = context.op_config.get("job_id", "00000000-0000-0000-0000-000000000000")
            if _demo:
                context.log.info(f"[ENABLE]  POST {_ep}/api/{_av}/job/{job_id}/execution/enable")
                context.log.info(f"  Response: 200 OK — job enabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                tk = os.environ.get(token_env, "")
                headers = _rundeck_headers(tk)
                _req.post(
                    f"{_ep}/api/{_av}/job/{job_id}/execution/enable",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"execution_id": dg.Field(str, is_required=False, default_value="0")})
        def abort_rundeck_execution(context: dg.OpExecutionContext):
            """Abort a running Rundeck execution. Config: {execution_id}."""
            execution_id = context.op_config.get("execution_id", "0")
            if _demo:
                context.log.info(f"[ABORT]   POST {_ep}/api/{_av}/execution/{execution_id}/abort")
                context.log.info(f"  Response: 200 OK — abort requested")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                tk = os.environ.get(token_env, "")
                headers = _rundeck_headers(tk)
                _req.post(
                    f"{_ep}/api/{_av}/execution/{execution_id}/abort",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_rundeck_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Rundeck's actual state, per project."""
            projects = sorted({j.project for j in self.jobs if j.project}) or ["default"]
            if _demo:
                for pr in projects:
                    context.log.info(f"[RECON] GET {_ep}/api/{_av}/project/{pr}/executions?recentFilter=1h&max=200")
                    context.log.info(f"  Project {pr}: 38 executions — 32 succeeded, 3 failed, 2 running, 1 aborted")
                    context.log.info(f"  Dagster: materializations for 30 of 32 'succeeded' executions")
                    context.log.info(f"  DRIFT:  2 succeeded in Rundeck but not in Dagster")
                    context.log.info(f"  ALERT:  3 executions in 'failed' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                tk = os.environ.get(token_env, "")
                headers = _rundeck_headers(tk)
                for pr in projects:
                    resp = _req.get(
                        f"{_ep}/api/{_av}/project/{pr}/executions",
                        params={"recentFilter": "1h", "max": 200},
                        headers=headers, verify=False, timeout=30,
                    )
                    resp.raise_for_status()
                    executions = resp.json().get("executions", [])
                    by_status: dict = {}
                    for ex in executions:
                        by_status.setdefault(ex.get("status", "?"), []).append(ex)
                    context.log.info(f"[RECON] Project {pr}: {len(executions)} executions")
                    for st, xs in sorted(by_status.items()):
                        context.log.info(f"  {st}: {len(xs)}")
                    for ex in by_status.get("failed", []):
                        context.log.warning(f"  ALERT: execution {ex.get('id')} ({ex.get('job', {}).get('name')}) — failed")

        @dg.job(description="Re-fire a Rundeck job (analog to restart-failed).")
        def rundeck_restart_execution():
            restart_failed_rundeck_execution()

        @dg.job(description="Disable a Rundeck job (no new executions).")
        def rundeck_disable_job():
            disable_rundeck_job()

        @dg.job(description="Enable a previously-disabled Rundeck job.")
        def rundeck_enable_job():
            enable_rundeck_job()

        @dg.job(description="Abort a running Rundeck execution.")
        def rundeck_abort_execution():
            abort_rundeck_execution()

        @dg.job(description="Reconcile Dagster state with Rundeck — detect drift, alert on failed.")
        def rundeck_reconciliation():
            reconcile_rundeck_state()

        recon_schedule = dg.ScheduleDefinition(
            name="rundeck_reconciliation_schedule",
            job=rundeck_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="rundeck_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Rundeck-initiated pipeline triggers. In production, "
                "a Rundeck job step calls Dagster's GraphQL API (launchRun mutation) "
                "to start runs after a scheduled execution completes."
            ),
        )
        def rundeck_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  Rundeck trigger detected (tick {tick})")
                context.log.info(f"  Source: Rundeck scheduled execution completion")
                context.log.info(f"  Action: In production, Rundeck job step calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[rundeck_external_monitor, rundeck_inbound_trigger],
            jobs=[
                rundeck_restart_execution, rundeck_disable_job,
                rundeck_enable_job, rundeck_abort_execution,
                rundeck_reconciliation,
            ],
            schedules=[recon_schedule],
        )
