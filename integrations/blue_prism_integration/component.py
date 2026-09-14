"""Blue Prism (SS&C Blue Prism) RPA integration component.

Bidirectional integration between Dagster and Blue Prism 7+ Web API:

  Dagster -> Blue Prism:
    - Authenticate via POST /api/v7/auth/authenticate (Basic) or X-API-Key
    - Start a session on a runtime resource (POST /api/v7/sessions)
    - Poll session status (GET /api/v7/sessions/{sessionId})
    - Retrieve session logs (GET /api/v7/sessions/{sessionId}/logs)
    - Soft-stop / hard-terminate / hold ops for operational control
    - Bearer token from the Basic-auth response used on subsequent calls

  Blue Prism -> Dagster:
    - A Blue Prism process can call Dagster's GraphQL API (launchRun mutation)
    - Startup parameters are passed via `inputs`; partition selected via {partition_key}

Each declared Blue Prism process becomes a daily-partitioned Dagster asset.
Operational tasks (restart / stop / terminate / hold / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the REST API on stdout — the whole
component runs end-to-end with zero external dependencies. Blue Prism does
NOT publish a public Docker image (Windows-native, enterprise-licensed to
SS&C customers), so demo_mode is how customers evaluate the shape before
pointing at a real environment.

Environment variables (production mode only):
  BLUE_PRISM_USER       — Blue Prism REST API username
  BLUE_PRISM_PASSWORD   — Blue Prism REST API password
  BLUE_PRISM_API_KEY    — alternative: X-API-Key from Blue Prism 7 Web API

Terminology map — for teams migrating from Control-M / RunMyJobs:
  Control-M Job          -> Blue Prism Process
  Control-M Folder       -> Blue Prism Environment / Application (business tag)
  Control-M Agent/Host   -> Blue Prism Runtime Resource
  Control-M runId        -> Blue Prism sessionId
  "Ended OK"             -> "Completed"

Session status is one of: Pending / Running / Terminated / Stopped /
Completed / Failed. Terminal states: Completed / Failed / Terminated / Stopped.

API reference:
  Blue Prism 7 Web API — Web API service published by the Blue Prism 7 platform.
  Older Blue Prism deployments (v6 and earlier) may only expose the legacy
  SOAP interface; this component targets the REST surface introduced in 7+.
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

class BluePrismProcessSpec(dg.Model, dg.Resolvable):
    """A Blue Prism process wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    process_id: str = Field(
        description="Blue Prism GUID for the process (as registered in the Blue Prism control room).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this Blue Prism process.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    resource_id: str = Field(
        default="",
        description="Target runtime resource GUID (analog to Control-M host / RMJ queue).",
    )
    resource_group: str = Field(
        default="",
        description="Business tag for the resource group (e.g. Finance, HR).",
    )
    application: str = Field(
        default="",
        description="Business tag for the enclosing Blue Prism application / environment.",
    )
    inputs: dict = Field(
        default_factory=dict,
        description="Blue Prism startup inputs. Values templated with {partition_key}.",
    )
    run_as: str = Field(default="svc_dagster", description="OS user Blue Prism runs the process as.")


class BluePrismSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Blue Prism process that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="blue_prism_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Blue Prism process that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_blue_prism(context: AssetExecutionContext, job: BluePrismProcessSpec, endpoint: str) -> dict:
    """Simulate the Blue Prism REST API lifecycle to stdout — no external deps."""
    session_id = str(uuid.uuid4())
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    # Template inputs with partition_key
    resolved_inputs = {}
    for k, v in (job.inputs or {}).items():
        if isinstance(v, str):
            resolved_inputs[k] = v.replace("{partition_key}", scheduled)
        else:
            resolved_inputs[k] = v

    start_payload = {
        "processId": job.process_id,
        "resourceId": job.resource_id,
        "inputs": resolved_inputs,
    }

    context.log.info(f"[AUTH]    POST {endpoint}/auth/authenticate — Basic (Blue Prism 7 Web API)")
    context.log.info(f"  Response: {{accessToken: 'ey***', tokenType: 'Bearer'}}")
    context.log.info(f"[START]   POST {endpoint}/sessions")
    context.log.info(f"  Payload: {json.dumps(start_payload, indent=2)}")
    context.log.info(f"  Response: {{sessionId: '{session_id}', status: 'Pending'}}")

    for state in ["Pending", "Pending", "Running", "Running", "Completed"]:
        context.log.info(f"[POLL]    GET {endpoint}/sessions/{session_id} → status={state}")

    context.log.info(f"[LOGS]    GET {endpoint}/sessions/{session_id}/logs → 428 entries")
    context.log.info(
        f"[DONE]    {job.process_id} → Completed "
        f"(sessionId: {session_id}, resource: {job.resource_id})"
    )

    return {
        "session_id": session_id,
        "status": "Completed",
        "scheduled_time": scheduled,
        "target": "blue_prism",
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _blue_prism_login(endpoint: str, user: str, password: str) -> str:
    """Exchange Basic credentials for a bearer accessToken."""
    import requests
    r = requests.post(
        f"{endpoint}/auth/authenticate",
        json={"authenticationType": "Basic", "username": user, "password": password},
        verify=False, timeout=30,
    )
    r.raise_for_status()
    return r.json()["accessToken"]


def _blue_prism_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_blue_prism(
    context: AssetExecutionContext,
    job: BluePrismProcessSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    api_key_env: str,
    poll_interval: int,
    poll_timeout: int,
    log_retrieval: bool,
) -> dict:
    """Real Blue Prism 7+ Web API session lifecycle.

    Prefers Basic authenticate (username/password -> bearer token). If only
    an API key is available (X-API-Key from the Blue Prism 7 Web API config)
    then the header-based auth path is used and no bearer exchange happens.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    api_key = os.environ.get(api_key_env)

    if user and password:
        token = _blue_prism_login(endpoint, user, password)
        headers = _blue_prism_headers(token)
    elif api_key:
        headers = {
            "X-API-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    else:
        raise RuntimeError(
            f"Missing credentials: set {user_env}+{password_env} or {api_key_env}"
        )

    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    resolved_inputs = {}
    for k, v in (job.inputs or {}).items():
        if isinstance(v, str):
            resolved_inputs[k] = v.replace("{partition_key}", scheduled)
        else:
            resolved_inputs[k] = v

    start_payload = {
        "processId": job.process_id,
        "resourceId": job.resource_id,
        "inputs": resolved_inputs,
    }
    context.log.info(f"[START] POST {endpoint}/sessions — processId={job.process_id}")
    resp = requests.post(
        f"{endpoint}/sessions",
        json=start_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    session_id = resp.json().get("sessionId") or resp.json().get("id", "unknown")
    context.log.info(f"  sessionId: {session_id}")

    start = time.time()
    terminal_states = {"Completed", "Failed", "Terminated", "Stopped"}
    status = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/sessions/{session_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "UNKNOWN")
        context.log.info(f"[POLL]  {job.process_id} → {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.process_id} timed out after {poll_timeout}s")

    if status != "Completed":
        raise RuntimeError(f"{job.process_id} finished: {status}")

    log_entries = 0
    if log_retrieval:
        try:
            log_resp = requests.get(
                f"{endpoint}/sessions/{session_id}/logs",
                headers=headers, verify=False, timeout=30,
            )
            if log_resp.ok:
                payload = log_resp.json()
                if isinstance(payload, list):
                    log_entries = len(payload)
                elif isinstance(payload, dict):
                    log_entries = len(payload.get("entries", []) or payload.get("logs", []))
                context.log.info(f"[LOGS]  {log_entries} entries")
        except Exception as e:
            context.log.warning(f"log retrieval failed: {e}")

    return {
        "session_id": session_id,
        "status": status,
        "scheduled_time": scheduled,
        "log_entries": log_entries,
        "target": "blue_prism",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class BluePrismIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """SS&C Blue Prism 7+ Web API integration.

    Each declared Blue Prism process becomes a daily-partitioned Dagster
    asset with a retry policy. Optional source-table declarations bring
    Blue Prism-managed tables into Dagster's lineage graph. Operational
    tasks (restart / stop / terminate / hold / reconcile) ship as Dagster
    jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no Blue Prism endpoint. Flip to
    `false` and set `BLUE_PRISM_USER` / `BLUE_PRISM_PASSWORD` (or
    `BLUE_PRISM_API_KEY`) to hit a real Blue Prism 7+ Web API instance.

    Blue Prism does not publish a public Docker image (Windows-native,
    enterprise-licensed to SS&C customers). The demo_mode simulator is
    the license-free path to evaluate the component end-to-end.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://blueprism.internal/api/v7",
        description="Blue Prism Web API base URL (used when demo_mode=false).",
    )
    jobs: List[BluePrismProcessSpec] = Field(
        default_factory=list,
        description="Blue Prism processes to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[BluePrismSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Blue Prism that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="blue_prism_integration",
        description="Dagster asset group for the process assets.",
    )

    blue_prism_user_env: str = Field(
        default="BLUE_PRISM_USER",
        description="Env var holding the Blue Prism REST username.",
    )
    blue_prism_password_env: str = Field(
        default="BLUE_PRISM_PASSWORD",
        description="Env var holding the Blue Prism REST password.",
    )
    blue_prism_api_key_env: str = Field(
        default="BLUE_PRISM_API_KEY",
        description="Env var holding the Blue Prism 7 Web API key (alternative to user+password).",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between Blue Prism session status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    log_retrieval: bool = Field(default=True, description="Retrieve session logs on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Blue Prism vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.blue_prism_user_env
        password_env = self.blue_prism_password_env
        api_key_env = self.blue_prism_api_key_env
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
                    kinds={"python", "blue-prism", "rpa"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"Blue Prism process: {_job.process_id}. "
                        f"Starts on resource {_job.resource_id or 'default'} "
                        f"(group: {_job.resource_group or 'default'}), "
                        f"polls for completion, retrieves logs."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "blue_prism",
                        "integration_pattern": "dagster-orchestrates-blue-prism",
                        "scheduler_owner": "Blue Prism",
                        "bp_resource_id": _job.resource_id,
                        "bp_resource_group": _job.resource_group,
                        "bp_application": _job.application,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_blue_prism(context, _job, _endpoint)
                    else:
                        result = _execute_blue_prism(
                            context, _job, _endpoint,
                            user_env, password_env, api_key_env,
                            poll_interval, poll_timeout, log_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_session_id": result.get("session_id", "N/A"),
                        "status": result.get("status", "N/A"),
                        "scheduled_time": result.get("scheduled_time", "N/A"),
                        "duration_seconds": duration,
                        "demo_mode": _demo,
                        "output_payload": dg.MetadataValue.json({
                            "vendor": "blue_prism",
                            "run_id": result.get("session_id", ""),
                            "status": result.get("status", ""),
                            "process_id": _job.process_id,
                            "resource_id": _job.resource_id,
                            "resource_group": _job.resource_group,
                            "inputs": _job.inputs,
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
                    kinds={"blue-prism", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Blue Prism: {_table.table_name}. "
                        f"Data is loaded by Blue Prism processes."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "blue_prism_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Blue Prism",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Blue Prism (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Blue Prism process'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="blue_prism_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Blue Prism for session completions Dagster didn't trigger.",
        )
        def blue_prism_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                session_id = str(uuid.uuid4())
                context.log.info(f"[DETECTED] External Blue Prism session")
                context.log.info(f"  Triggered by: Blue Prism control room schedule")
                context.log.info(f"  Status: Completed")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  sessionId: {session_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "blue_prism_process"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_session_id": dg.MetadataValue.text(session_id),
                        "platform": dg.MetadataValue.text("blue_prism"),
                        "triggered_by": dg.MetadataValue.text("Blue Prism control room schedule"),
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
                api_key = os.environ.get(api_key_env)
                headers = None
                try:
                    if user and pw:
                        headers = _blue_prism_headers(_blue_prism_login(_ep, user, pw))
                    elif api_key:
                        headers = {
                            "X-API-Key": api_key,
                            "Accept": "application/json",
                            "Content-Type": "application/json",
                        }
                    if headers:
                        resp = requests.get(
                            f"{_ep}/sessions",
                            params={"status": "Completed", "limit": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        payload = resp.json()
                        sessions = payload if isinstance(payload, list) else payload.get("sessions", [])
                        for s in sessions[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"bp_{(s.get('processName') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_session_id": dg.MetadataValue.text(str(s.get("sessionId", "unknown"))),
                                    "status": dg.MetadataValue.text(s.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                except Exception as e:
                    context.log.warning(f"Blue Prism monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={
            "process_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
            "resource_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
        })
        def restart_blue_prism_session(context: dg.OpExecutionContext):
            """Restart / start a fresh Blue Prism session. Config: {process_id, resource_id}."""
            process_id = context.op_config.get("process_id")
            resource_id = context.op_config.get("resource_id")
            if _demo:
                new_session = str(uuid.uuid4())
                context.log.info(f"[RESTART] POST {_ep}/sessions")
                context.log.info(f"  Payload: {{processId: '{process_id}', resourceId: '{resource_id}'}}")
                context.log.info(f"  Response: {{sessionId: '{new_session}', status: 'Pending'}}")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _blue_prism_headers(_blue_prism_login(_ep, u or "", p or ""))
                _req.post(
                    f"{_ep}/sessions",
                    json={"processId": process_id, "resourceId": resource_id, "inputs": {}},
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] Fresh session started for process {process_id}")

        @dg.op(config_schema={"session_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def stop_blue_prism_session(context: dg.OpExecutionContext):
            """Soft-stop a Blue Prism session. Config: {session_id}."""
            session_id = context.op_config.get("session_id")
            if _demo:
                context.log.info(f"[STOP]    POST {_ep}/sessions/{session_id}/stop")
                context.log.info(f"  Response: 200 OK — soft stop requested")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _blue_prism_headers(_blue_prism_login(_ep, u or "", p or ""))
                _req.post(
                    f"{_ep}/sessions/{session_id}/stop",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"session_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def terminate_blue_prism_session(context: dg.OpExecutionContext):
            """Hard-terminate a Blue Prism session. Config: {session_id}."""
            session_id = context.op_config.get("session_id")
            if _demo:
                context.log.info(f"[TERMINATE] POST {_ep}/sessions/{session_id}/terminate")
                context.log.info(f"  Response: 200 OK — session terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _blue_prism_headers(_blue_prism_login(_ep, u or "", p or ""))
                _req.post(
                    f"{_ep}/sessions/{session_id}/terminate",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"process_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000")})
        def hold_blue_prism_process(context: dg.OpExecutionContext):
            """Hold (disable) a Blue Prism process. Config: {process_id}."""
            process_id = context.op_config.get("process_id")
            if _demo:
                context.log.info(f"[HOLD]    POST {_ep}/processes/{process_id}/setEnabled")
                context.log.info(f"  Payload: {{enabled: false}}")
                context.log.info(f"  Response: 200 OK — process disabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _blue_prism_headers(_blue_prism_login(_ep, u or "", p or ""))
                _req.post(
                    f"{_ep}/processes/{process_id}/setEnabled",
                    json={"enabled": False},
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_blue_prism_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Blue Prism's actual state."""
            if _demo:
                context.log.info(f"[RECON]   GET {_ep}/sessions?startedAfter=1h&limit=200")
                context.log.info(f"  Blue Prism: 41 sessions — 33 Completed, 3 Failed, 2 Terminated, 3 Running")
                context.log.info(f"  Dagster: materializations for 31 of 33 'Completed' sessions")
                context.log.info(f"  DRIFT: 2 sessions completed in Blue Prism but not in Dagster")
                context.log.info(f"  ALERT: 3 sessions in 'Failed' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                api_key = os.environ.get(api_key_env)
                if u and p:
                    headers = _blue_prism_headers(_blue_prism_login(_ep, u, p))
                elif api_key:
                    headers = {
                        "X-API-Key": api_key,
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    }
                else:
                    context.log.warning("No Blue Prism credentials configured — skipping reconciliation")
                    return
                resp = _req.get(
                    f"{_ep}/sessions",
                    params={"status": "Completed,Failed", "limit": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                payload = resp.json()
                sessions = payload if isinstance(payload, list) else payload.get("sessions", [])
                by_status: dict = {}
                for s in sessions:
                    by_status.setdefault(s.get("status", "?"), []).append(s)
                context.log.info(f"[RECON] Blue Prism: {len(sessions)} sessions")
                for st, ss in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(ss)}")
                for s in by_status.get("Failed", []):
                    context.log.warning(f"  ALERT: session {s.get('sessionId')} — Failed")

        @dg.job(description="Restart / start a fresh Blue Prism session.")
        def blue_prism_restart_session():
            restart_blue_prism_session()

        @dg.job(description="Soft-stop a running Blue Prism session.")
        def blue_prism_stop_session():
            stop_blue_prism_session()

        @dg.job(description="Hard-terminate a running Blue Prism session.")
        def blue_prism_terminate_session():
            terminate_blue_prism_session()

        @dg.job(description="Hold (disable) a Blue Prism process.")
        def blue_prism_hold_process():
            hold_blue_prism_process()

        @dg.job(description="Reconcile Dagster state with Blue Prism — detect drift.")
        def blue_prism_reconciliation():
            reconcile_blue_prism_state()

        recon_schedule = dg.ScheduleDefinition(
            name="blue_prism_reconciliation_schedule",
            job=blue_prism_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="blue_prism_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Blue Prism-initiated pipeline triggers. In production, "
                "a Blue Prism process calls Dagster's GraphQL API (launchRun mutation) "
                "to start runs after a scheduled session completes."
            ),
        )
        def blue_prism_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  Blue Prism trigger detected (tick {tick})")
                context.log.info(f"  Source: Blue Prism scheduled session completion")
                context.log.info(f"  Action: In production, Blue Prism calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[blue_prism_external_monitor, blue_prism_inbound_trigger],
            jobs=[
                blue_prism_restart_session, blue_prism_stop_session,
                blue_prism_terminate_session, blue_prism_hold_process,
                blue_prism_reconciliation,
            ],
            schedules=[recon_schedule],
        )
