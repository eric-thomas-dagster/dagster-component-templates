"""UiPath Orchestrator integration component.

Bidirectional integration between Dagster and UiPath Orchestrator:

  Dagster -> UiPath:
    - OAuth2 client-credentials exchange for a Bearer token
    - Start a Process (Release) via
      POST /odata/Jobs/UiPath.Server.Configuration.OData.StartJobs
    - Poll the resulting Job status via GET /odata/Jobs({id})
    - Retrieve OutputArguments on completion
    - Stop / kill / schedule-disable ops for operational control
    - Bearer token in Authorization header + optional
      X-UIPATH-OrganizationUnitId when targeting a specific folder

  UiPath -> Dagster:
    - A UiPath process's post-step can call Dagster's GraphQL API
      (launchRun mutation); Parameters flow as InputArguments
    - The inbound-trigger sensor confirms the trigger was received

Each declared Release becomes a daily-partitioned Dagster asset.
Operational tasks (restart / soft-stop / kill / schedule-disable /
reconcile) ship as Dagster jobs backed by ops so the customer can
wire them into the UI or Dagster+ Automations.

`demo_mode: true` (default) simulates the Orchestrator REST API on
stdout — the whole component runs end-to-end with zero external
dependencies (and zero UiPath licensing).

Environment variables (production mode only):
  UIPATH_CLIENT_ID      — External Application client_id
  UIPATH_CLIENT_SECRET  — External Application client_secret

Terminology map — for teams migrating from Control-M / RunMyJobs:
  UiPath Process (Release) -> Control-M Job          -> RMJ JobDefinition
  UiPath Folder            -> Control-M Folder       -> RMJ Application
  UiPath Robot / Machine   -> Control-M Agent/Host   -> RMJ Queue
  UiPath Job.Key           -> Control-M runId        -> RMJ processId
  UiPath 'Successful'      -> Control-M 'Ended OK'   -> RMJ 'Completed'
  UiPath 'Faulted'         -> Control-M 'Ended Not OK'-> RMJ 'Error'

API reference:
  https://docs.uipath.com/orchestrator/reference/api-references
  (exact REST paths and payload shapes vary between UiPath Automation
  Cloud, standalone on-prem Orchestrator, and Orchestrator versions —
  verify against your instance when flipping demo_mode to false; see
  README for the modern surface used by this component.)
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

class UiPathProcessSpec(dg.Model, dg.Resolvable):
    """A UiPath Process (Release) wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    release_key: str = Field(
        description="UiPath ReleaseKey — the unique key of a specific Process Release in Orchestrator.",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Release.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    folder: str = Field(
        default="",
        description="UiPath Folder name (display label; sets X-UIPATH-OrganizationUnitId when folder_id known).",
    )
    folder_id: int = Field(
        default=0,
        description="Numeric UiPath Folder id (OrganizationUnitId) — required to scope the API call to a folder.",
    )
    robot_ids: list = Field(
        default_factory=list,
        description="Target robot IDs. Empty list means 'any available robot' (Strategy=ModernJobsCount).",
    )
    input_arguments: dict = Field(
        default_factory=dict,
        description="JSON dict passed as startInfo.InputArguments. String values are templated with {partition_key}.",
    )
    machine_group: str = Field(
        default="",
        description="Optional UiPath Machine Template / Modern Folder machine group name.",
    )
    run_as: str = Field(
        default="svc_dagster",
        description="OS user / Robot account label the process runs as (metadata only).",
    )


class UiPathSourceTableSpec(dg.Model, dg.Resolvable):
    """A table populated by a UiPath process that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(
        default="uipath_managed_data",
        description="Dagster asset group for this table.",
    )
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the UiPath process that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_uipath(context: AssetExecutionContext, job: UiPathProcessSpec, endpoint: str) -> dict:
    """Simulate the Orchestrator REST API lifecycle to stdout — no external deps."""
    job_id = int(uuid.uuid4().int % 10_000_000)
    job_key = str(uuid.uuid4())
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    # Template partition_key into input_arguments string values
    templated_inputs = {}
    for k, v in (job.input_arguments or {}).items():
        if isinstance(v, str):
            templated_inputs[k] = v.replace("{partition_key}", scheduled)
        else:
            templated_inputs[k] = v

    start_info = {
        "ReleaseKey": job.release_key,
        "Strategy": "ModernJobsCount" if not job.robot_ids else "Specific",
        "RobotIds": job.robot_ids,
        "NoOfRobots": 0 if not job.robot_ids else len(job.robot_ids),
        "JobsCount": 1,
        "InputArguments": json.dumps(templated_inputs),
    }

    context.log.info(f"[AUTH]    POST {endpoint}/identity_/connect/token — OAuth2 client credentials")
    context.log.info(f"  Response: {{access_token: 'ey***', token_type: 'Bearer', expires_in: 3600}}")

    if job.folder_id:
        context.log.info(f"[FOLDER]  X-UIPATH-OrganizationUnitId: {job.folder_id}")

    context.log.info(f"[START]   POST {endpoint}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs")
    context.log.info(f"  Payload: {{startInfo: {json.dumps(start_info)}}}")
    context.log.info(f"  Response: {{value: [{{Id: {job_id}, Key: '{job_key}', State: 'Pending'}}]}}")

    for state in ["Pending", "Pending", "Running", "Running", "Successful"]:
        context.log.info(f"[POLL]    GET {endpoint}/odata/Jobs({job_id}) -> State={state}")

    output_arguments = json.dumps({"OutStatus": "OK", "OutScheduledDate": scheduled})
    context.log.info(f"[OUTPUT]  {len(output_arguments)} chars in OutputArguments")
    context.log.info(f"[DONE]    {job.release_key} -> Successful (Id: {job_id}, folder: {job.folder or 'default'})")

    return {
        "job_id": job_id,
        "job_key": job_key,
        "status": "Successful",
        "scheduled_time": scheduled,
        "target": "uipath",
        "output_arguments": output_arguments,
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _uipath_get_token(endpoint: str, client_id: str, client_secret: str) -> str:
    import requests
    r = requests.post(
        f"{endpoint}/identity_/connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "OR.Jobs OR.Execution OR.Folders",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _uipath_headers(token: str, folder_id: int = 0) -> dict:
    h = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if folder_id:
        h["X-UIPATH-OrganizationUnitId"] = str(folder_id)
    return h


def _execute_uipath(
    context: AssetExecutionContext,
    job: UiPathProcessSpec,
    endpoint: str,
    client_id_env: str,
    client_secret_env: str,
    poll_interval: int,
    poll_timeout: int,
) -> dict:
    """Real UiPath Orchestrator REST lifecycle.

    Endpoint paths follow the modern OData surface documented at
    https://docs.uipath.com/orchestrator/reference/api-references .
    Cloud + on-prem + Orchestrator versions differ in prefix and
    payload shape; override `endpoint` to the full base (including
    `/{organization}/{tenant}/orchestrator_`) for cloud, or
    `https://<host>/` for on-prem.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    if not client_id or not client_secret:
        raise RuntimeError(f"Missing {client_id_env} and/or {client_secret_env} environment variables")

    token = _uipath_get_token(endpoint, client_id, client_secret)
    headers = _uipath_headers(token, job.folder_id)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    templated_inputs = {}
    for k, v in (job.input_arguments or {}).items():
        templated_inputs[k] = v.replace("{partition_key}", scheduled) if isinstance(v, str) else v

    start_info = {
        "ReleaseKey": job.release_key,
        "Strategy": "ModernJobsCount" if not job.robot_ids else "Specific",
        "RobotIds": job.robot_ids,
        "NoOfRobots": 0 if not job.robot_ids else len(job.robot_ids),
        "JobsCount": 1,
        "InputArguments": json.dumps(templated_inputs),
    }

    context.log.info(f"[START]  POST {endpoint}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs — {job.release_key}")
    resp = requests.post(
        f"{endpoint}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs",
        json={"startInfo": start_info},
        headers=headers,
        verify=False,
        timeout=30,
    )
    resp.raise_for_status()
    started = resp.json().get("value", [])
    if not started:
        raise RuntimeError(f"UiPath StartJobs returned empty value array for {job.release_key}")
    job_id = started[0].get("Id")
    job_key = started[0].get("Key", "unknown")
    context.log.info(f"  Id: {job_id}  Key: {job_key}")

    start = time.time()
    terminal_states = {"Faulted", "Successful", "Stopped"}
    status = "UNKNOWN"
    output_arguments = ""

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/odata/Jobs({job_id})",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        status = body.get("State", "UNKNOWN")
        context.log.info(f"[POLL]   Job({job_id}) -> {status}")
        if status in terminal_states:
            output_arguments = body.get("OutputArguments", "") or ""
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.release_key} timed out after {poll_timeout}s")

    if status != "Successful":
        raise RuntimeError(f"{job.release_key} finished: {status}")

    context.log.info(f"[OUTPUT] {len(output_arguments)} chars in OutputArguments")
    return {
        "job_id": job_id,
        "job_key": job_key,
        "status": status,
        "scheduled_time": scheduled,
        "output_arguments": output_arguments,
        "target": "uipath",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class UiPathOrchestratorIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """UiPath Orchestrator REST API integration — the RPA-domain sibling
    of `runmyjobs_integration` and `controlm_integration`.

    Each declared Release becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    UiPath-populated tables into Dagster's lineage graph. Operational
    tasks (restart / soft-stop / kill / schedule-disable / reconcile)
    ship as Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so
    the whole component runs end-to-end with no Orchestrator endpoint
    and zero UiPath licensing. Flip to `false` and set
    `UIPATH_CLIENT_ID` / `UIPATH_CLIENT_SECRET` to hit a real
    Orchestrator instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://cloud.uipath.com/organization/tenant/orchestrator_",
        description="UiPath Orchestrator base URL (used when demo_mode=false). Cloud: https://cloud.uipath.com/{org}/{tenant}/orchestrator_ — on-prem: https://<host>",
    )
    jobs: List[UiPathProcessSpec] = Field(
        default_factory=list,
        description="UiPath Releases to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[UiPathSourceTableSpec] = Field(
        default_factory=list,
        description="Tables populated by UiPath processes that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="uipath_orchestrator_integration",
        description="Dagster asset group for the job assets.",
    )

    uipath_client_id_env: str = Field(
        default="UIPATH_CLIENT_ID",
        description="Env var holding the UiPath External Application client_id.",
    )
    uipath_client_secret_env: str = Field(
        default="UIPATH_CLIENT_SECRET",
        description="Env var holding the UiPath External Application client_secret.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between UiPath job status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the UiPath vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        client_id_env = self.uipath_client_id_env
        client_secret_env = self.uipath_client_secret_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds

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
                    kinds={"python", "uipath", "rpa"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"UiPath Process (Release): {_job.release_key}. "
                        f"Runs in folder {_job.folder or 'default'} "
                        f"(machine group: {_job.machine_group or 'default'}), "
                        f"polls for terminal state, captures OutputArguments."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "uipath",
                        "integration_pattern": "dagster-orchestrates-uipath",
                        "scheduler_owner": "UiPath Orchestrator",
                        "uipath_folder": _job.folder,
                        "uipath_folder_id": _job.folder_id,
                        "uipath_release_key": _job.release_key,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_uipath(context, _job, _endpoint)
                    else:
                        result = _execute_uipath(
                            context, _job, _endpoint,
                            client_id_env, client_secret_env,
                            poll_interval, poll_timeout,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_job_id": str(result.get("job_id", "N/A")),
                        "external_job_key": result.get("job_key", "N/A"),
                        "status": result.get("status", "N/A"),
                        "scheduled_time": result.get("scheduled_time", "N/A"),
                        "output_arguments_chars": len(result.get("output_arguments", "") or ""),
                        "duration_seconds": duration,
                        "demo_mode": _demo,
                        "output_payload": dg.MetadataValue.json({
                            "vendor": "uipath",
                            "run_id": str(result.get("job_id", "")),
                            "run_key": result.get("job_key", ""),
                            "status": result.get("status", ""),
                            "output_arguments": result.get("output_arguments", ""),
                            "folder_id": _job.folder_id,
                            "folder": _job.folder,
                            "release_key": _job.release_key,
                        }),
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
                    kinds={"uipath", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table populated by UiPath: {_table.table_name}. "
                        f"Data is written by UiPath processes."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "uipath_managed",
                        "table_name": _table.table_name,
                        "managed_by": "UiPath Orchestrator",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: UiPath Orchestrator (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown UiPath process'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="uipath_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors UiPath Orchestrator for job completions Dagster didn't trigger.",
        )
        def uipath_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                job_id = int(uuid.uuid4().int % 10_000_000)
                context.log.info(f"[DETECTED] External UiPath job")
                context.log.info(f"  Triggered by: UiPath schedule / queue trigger")
                context.log.info(f"  State: Successful")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  Id: {job_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "uipath_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_job_id": dg.MetadataValue.text(str(job_id)),
                        "platform": dg.MetadataValue.text("uipath"),
                        "triggered_by": dg.MetadataValue.text("UiPath schedule / queue"),
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
                cid = os.environ.get(client_id_env)
                cs = os.environ.get(client_secret_env)
                if cid and cs:
                    try:
                        token = _uipath_get_token(_ep, cid, cs)
                        headers = _uipath_headers(token)
                        resp = requests.get(
                            f"{_ep}/odata/Jobs",
                            params={"$filter": "State eq 'Successful'", "$top": 20, "$orderby": "StartTime desc"},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for pr in resp.json().get("value", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"uipath_{str(pr.get('ReleaseName') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_job_id": dg.MetadataValue.text(str(pr.get("Id", "unknown"))),
                                    "state": dg.MetadataValue.text(pr.get("State", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"UiPath monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={
            "release_key": dg.Field(str, is_required=False, default_value="release-unknown"),
            "folder": dg.Field(str, is_required=False, default_value=""),
        })
        def restart_uipath_job(context: dg.OpExecutionContext):
            """Start a new UiPath job for a Release. Config: {release_key, folder}."""
            release_key = context.op_config.get("release_key", "release-unknown")
            folder = context.op_config.get("folder", "")
            if _demo:
                context.log.info(f"[RESTART] POST {_ep}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs")
                context.log.info(f"  Payload: {{startInfo: {{ReleaseKey: '{release_key}', Strategy: 'ModernJobsCount', JobsCount: 1}}}}")
                context.log.info(f"  Folder: {folder or 'default'}")
                context.log.info(f"  Response: 201 Created — job started")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                cid, cs = os.environ.get(client_id_env), os.environ.get(client_secret_env)
                if not cid or not cs:
                    raise RuntimeError(f"Missing {client_id_env}/{client_secret_env}")
                token = _uipath_get_token(_ep, cid, cs)
                headers = _uipath_headers(token)
                _req.post(
                    f"{_ep}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs",
                    json={"startInfo": {"ReleaseKey": release_key, "Strategy": "ModernJobsCount", "JobsCount": 1}},
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] {release_key} started in folder '{folder or 'default'}'")

        @dg.op(config_schema={"job_id": dg.Field(int, is_required=False, default_value=0)})
        def stop_uipath_job_soft(context: dg.OpExecutionContext):
            """Soft-stop a running UiPath job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", 0)
            if _demo:
                context.log.info(f"[STOP]    POST {_ep}/odata/Jobs({job_id})/UiPath.Server.Configuration.OData.StopJob")
                context.log.info(f"  Payload: {{strategy: 'SoftStop'}}")
                context.log.info(f"  Response: 200 OK — job stopping gracefully")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                cid, cs = os.environ.get(client_id_env), os.environ.get(client_secret_env)
                token = _uipath_get_token(_ep, cid or "", cs or "")
                headers = _uipath_headers(token)
                _req.post(
                    f"{_ep}/odata/Jobs({job_id})/UiPath.Server.Configuration.OData.StopJob",
                    json={"strategy": "SoftStop"}, headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_id": dg.Field(int, is_required=False, default_value=0)})
        def stop_uipath_job_kill(context: dg.OpExecutionContext):
            """Kill a running UiPath job. Config: {job_id}."""
            job_id = context.op_config.get("job_id", 0)
            if _demo:
                context.log.info(f"[KILL]    POST {_ep}/odata/Jobs({job_id})/UiPath.Server.Configuration.OData.StopJob")
                context.log.info(f"  Payload: {{strategy: 'Kill'}}")
                context.log.info(f"  Response: 200 OK — job terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                cid, cs = os.environ.get(client_id_env), os.environ.get(client_secret_env)
                token = _uipath_get_token(_ep, cid or "", cs or "")
                headers = _uipath_headers(token)
                _req.post(
                    f"{_ep}/odata/Jobs({job_id})/UiPath.Server.Configuration.OData.StopJob",
                    json={"strategy": "Kill"}, headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"schedule_id": dg.Field(int, is_required=False, default_value=0)})
        def disable_uipath_schedule(context: dg.OpExecutionContext):
            """Disable a UiPath schedule (ProcessSchedule). Config: {schedule_id}."""
            schedule_id = context.op_config.get("schedule_id", 0)
            if _demo:
                context.log.info(f"[DISABLE] POST {_ep}/odata/ProcessSchedules({schedule_id})/UiPath.Server.Configuration.OData.SetEnabled")
                context.log.info(f"  Payload: {{enabled: false}}")
                context.log.info(f"  Response: 204 No Content — schedule disabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                cid, cs = os.environ.get(client_id_env), os.environ.get(client_secret_env)
                token = _uipath_get_token(_ep, cid or "", cs or "")
                headers = _uipath_headers(token)
                _req.post(
                    f"{_ep}/odata/ProcessSchedules({schedule_id})/UiPath.Server.Configuration.OData.SetEnabled",
                    json={"enabled": False}, headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_uipath_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with UiPath Orchestrator's actual state."""
            if _demo:
                context.log.info(f"[RECON]  GET {_ep}/odata/Jobs?$top=200&$orderby=StartTime desc")
                context.log.info(f"  UiPath: 48 jobs — 39 Successful, 3 Faulted, 4 Running, 2 Pending")
                context.log.info(f"  Dagster: materializations for 37 of 39 'Successful' jobs")
                context.log.info(f"  DRIFT: 2 jobs Successful in UiPath but not in Dagster")
                context.log.info(f"  ALERT: 3 jobs in 'Faulted' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                cid, cs = os.environ.get(client_id_env), os.environ.get(client_secret_env)
                token = _uipath_get_token(_ep, cid or "", cs or "")
                headers = _uipath_headers(token)
                resp = _req.get(
                    f"{_ep}/odata/Jobs",
                    params={"$top": 200, "$orderby": "StartTime desc"},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                jobs = resp.json().get("value", [])
                by_state: dict = {}
                for jr in jobs:
                    by_state.setdefault(jr.get("State", "?"), []).append(jr)
                context.log.info(f"[RECON]  UiPath: {len(jobs)} jobs")
                for st, jrs in sorted(by_state.items()):
                    context.log.info(f"  {st}: {len(jrs)}")
                for jr in by_state.get("Faulted", []):
                    context.log.warning(f"  ALERT: Job Id={jr.get('Id')} Release={jr.get('ReleaseName')} — Faulted")

        @dg.job(description="Start a new UiPath job for a Release.")
        def uipath_restart_job():
            restart_uipath_job()

        @dg.job(description="Soft-stop a running UiPath job.")
        def uipath_stop_job_soft():
            stop_uipath_job_soft()

        @dg.job(description="Kill a running UiPath job.")
        def uipath_stop_job_kill():
            stop_uipath_job_kill()

        @dg.job(description="Disable a UiPath schedule (ProcessSchedule).")
        def uipath_disable_schedule():
            disable_uipath_schedule()

        @dg.job(description="Reconcile Dagster state with UiPath Orchestrator — detect drift.")
        def uipath_reconciliation():
            reconcile_uipath_state()

        recon_schedule = dg.ScheduleDefinition(
            name="uipath_reconciliation_schedule",
            job=uipath_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="uipath_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for UiPath-initiated pipeline triggers. In production, "
                "a UiPath process's post-step calls Dagster's GraphQL API "
                "(launchRun mutation) to start runs after a UiPath job completes."
            ),
        )
        def uipath_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND] UiPath trigger detected (tick {tick})")
                context.log.info(f"  Source: UiPath job completion post-step")
                context.log.info(f"  Action: In production, UiPath calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[uipath_external_monitor, uipath_inbound_trigger],
            jobs=[
                uipath_restart_job, uipath_stop_job_soft,
                uipath_stop_job_kill, uipath_disable_schedule,
                uipath_reconciliation,
            ],
            schedules=[recon_schedule],
        )
