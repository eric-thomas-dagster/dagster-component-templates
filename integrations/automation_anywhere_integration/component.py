"""Automation Anywhere (A360 / AAI) Control Room integration component.

Bidirectional integration between Dagster and Automation Anywhere's
Control Room (A360 on-prem or Automation Anywhere Cloud / AAI):

  Dagster -> Automation Anywhere:
    - Authenticate (POST /v1/authentication) -> JWT token
    - Deploy a bot (POST /v3/automations/deploy)
    - Poll deployment / execution status
        (GET /v3/activity/execution/{id})
    - Retrieve execution logs (GET /v3/activity/execution/{id}/logs)
    - Stop / pause / resume ops for operational control
    - Auth header: X-Authorization: <jwt>

  Automation Anywhere -> Dagster:
    - AA callback URL (callbackInfo on the deploy payload) posts to a
      Dagster webhook / GraphQL launchRun mutation on terminal status

Each declared Bot (fileId) becomes a daily-partitioned Dagster asset.
Operational tasks (redeploy / pause / resume / stop / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the REST API on stdout -- the
whole component runs end-to-end with zero external dependencies.

Environment variables (production mode only):
  AUTOMATION_ANYWHERE_USER      -- Control Room username
  AUTOMATION_ANYWHERE_PASSWORD  -- Control Room password (or apiKey)

Terminology map -- for teams migrating from Control-M / RunMyJobs:
  Control-M Job          -> RunMyJobs JobDefinition -> AA Bot (File)
  Control-M Folder       -> RunMyJobs Application   -> AA Workspace / Folder
  Control-M Agent/Host   -> RunMyJobs Queue         -> AA Device Pool
  Control-M runId        -> RunMyJobs processId     -> AA Deployment (executionId)
  "Ended OK"             -> "Completed"             -> "COMPLETED"
  "Ended Not OK"         -> "Error"                 -> "FAILED"

API reference:
  https://docs.automationanywhere.com/ (endpoints vary between Cloud (AAI)
  and on-prem A360 versions -- verify against your instance when flipping
  demo_mode to false; see README).
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

class AutomationAnywhereBotSpec(dg.Model, dg.Resolvable):
    """An Automation Anywhere Bot (File) wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    file_id: int = Field(
        description="Numeric file ID of the bot in the Control Room repository.",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this bot.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    workspace: str = Field(
        default="",
        description="Business tag for the bot's workspace. A360 has 'Public' and 'Private' workspaces.",
    )
    application: str = Field(
        default="",
        description="Business tag for the bot's application (e.g. FINANCE, HR).",
    )
    device_pool_id: int = Field(
        default=0,
        description="Numeric device pool ID for target unattended bots.",
    )
    run_as_user_ids: list = Field(
        default_factory=list,
        description="Numeric user IDs to run the bot as (Control Room user IDs).",
    )
    bot_input: dict = Field(
        default_factory=dict,
        description="Bot input values. String values templated with {partition_key}.",
    )
    run_as: str = Field(default="svc_dagster", description="OS / Control Room user the bot runs as.")


class AutomationAnywhereSourceTableSpec(dg.Model, dg.Resolvable):
    """A table populated by an Automation Anywhere bot that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(
        default="automation_anywhere_managed_data",
        description="Dagster asset group for this table.",
    )
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the AA bot that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_automation_anywhere(
    context: AssetExecutionContext,
    bot: AutomationAnywhereBotSpec,
    endpoint: str,
) -> dict:
    """Simulate the REST API lifecycle to stdout -- no external deps."""
    deployment_id = str(uuid.uuid4())
    execution_id = int(uuid.uuid4().int % 10_000_000)
    partition_key = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    # Template bot_input string values with the partition_key
    templated_input = {
        k: (v.format(partition_key=partition_key) if isinstance(v, str) else v)
        for k, v in (bot.bot_input or {}).items()
    }

    deploy_payload = {
        "fileId": bot.file_id,
        "runAsUserIds": bot.run_as_user_ids,
        "poolIds": [bot.device_pool_id] if bot.device_pool_id else [],
        "overrideDefaultDevice": False,
        "callbackInfo": {
            "url": "https://dagster.cloud/webhooks/automation-anywhere",
            "headers": {"X-Dagster-Run-Id": context.run_id},
        },
        "botInput": templated_input,
    }

    context.log.info(f"[AUTH]    POST {endpoint}/v1/authentication")
    context.log.info(f"  Response: {{token: 'ey***', tokenType: 'Bearer'}}")
    context.log.info(f"[DEPLOY]  POST {endpoint}/v3/automations/deploy")
    context.log.info(f"  Payload: {json.dumps({'fileId': bot.file_id, 'poolIds': [bot.device_pool_id] if bot.device_pool_id else [], 'botInput': templated_input})}")
    context.log.info(f"  Response: {{deploymentId: '{deployment_id}', executionId: {execution_id}}}")

    for state in ["DEPLOYED", "QUEUED", "RUNNING", "RUNNING", "COMPLETED"]:
        context.log.info(f"[POLL]    GET {endpoint}/v3/activity/execution/{execution_id} → status={state}")

    context.log.info(f"[LOGS]    GET {endpoint}/v3/activity/execution/{execution_id}/logs → 412 entries")
    context.log.info(
        f"[DONE]    file_id={bot.file_id} → COMPLETED "
        f"(executionId: {execution_id}, workspace: {bot.workspace or 'Public'})"
    )

    return {
        "deployment_id": deployment_id,
        "execution_id": execution_id,
        "status": "COMPLETED",
        "partition_key": partition_key,
        "target": "automation_anywhere",
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _aa_login(endpoint: str, user: str, password: str) -> str:
    import requests
    r = requests.post(
        f"{endpoint}/v1/authentication",
        json={"username": user, "password": password},
        verify=False,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["token"]


def _aa_headers(token: str) -> dict:
    return {
        "X-Authorization": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_automation_anywhere(
    context: AssetExecutionContext,
    bot: AutomationAnywhereBotSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    log_retrieval: bool,
) -> dict:
    """Real Automation Anywhere Control Room REST API lifecycle.

    Endpoints vary between A360 on-prem versions and Automation Anywhere
    Cloud (AAI). If your Control Room uses a different prefix or payload
    shape, override `endpoint` or fork `_execute_automation_anywhere` in
    `component.py` to match.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    token = _aa_login(endpoint, user, password)
    headers = _aa_headers(token)

    partition_key = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    templated_input = {
        k: (v.format(partition_key=partition_key) if isinstance(v, str) else v)
        for k, v in (bot.bot_input or {}).items()
    }

    deploy_payload = {
        "fileId": bot.file_id,
        "runAsUserIds": bot.run_as_user_ids,
        "poolIds": [bot.device_pool_id] if bot.device_pool_id else [],
        "overrideDefaultDevice": False,
        "callbackInfo": {
            "url": f"https://dagster.cloud/webhooks/automation-anywhere",
            "headers": {"X-Dagster-Run-Id": context.run_id},
        },
        "botInput": templated_input,
    }
    context.log.info(f"[DEPLOY] POST {endpoint}/v3/automations/deploy -- fileId={bot.file_id}")
    resp = requests.post(
        f"{endpoint}/v3/automations/deploy",
        json=deploy_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    deployment_id = body.get("deploymentId", "unknown")
    execution_id = body.get("executionId") or body.get("id", "unknown")
    context.log.info(f"  deploymentId: {deployment_id}  executionId: {execution_id}")

    start = time.time()
    terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "DEPLOY_FAILED", "TIMED_OUT"}
    status = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/v3/activity/execution/{execution_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "UNKNOWN")
        context.log.info(f"[POLL]   fileId={bot.file_id} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"fileId={bot.file_id} timed out after {poll_timeout}s")

    if status != "COMPLETED":
        raise RuntimeError(f"fileId={bot.file_id} finished: {status}")

    log_entries = 0
    if log_retrieval:
        try:
            log_resp = requests.get(
                f"{endpoint}/v3/activity/execution/{execution_id}/logs",
                headers=headers, verify=False, timeout=30,
            )
            if log_resp.ok:
                log_json = log_resp.json()
                log_lines = log_json.get("list", []) if isinstance(log_json, dict) else []
                log_entries = len(log_lines)
                context.log.info(f"[LOGS]   {log_entries} entries")
                for line in log_lines[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"log retrieval failed: {e}")

    return {
        "deployment_id": deployment_id,
        "execution_id": execution_id,
        "status": status,
        "partition_key": partition_key,
        "log_entries": log_entries,
        "target": "automation_anywhere",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class AutomationAnywhereIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Automation Anywhere (A360 / AAI) Control Room REST API integration.

    Each declared Bot (fileId) becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    AA-managed tables into Dagster's lineage graph. Operational tasks
    (redeploy / pause / resume / stop / reconcile) ship as Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no Control Room endpoint. Flip
    to `false` and set `AUTOMATION_ANYWHERE_USER` /
    `AUTOMATION_ANYWHERE_PASSWORD` to hit a real Control Room.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://control-room.internal",
        description="Automation Anywhere Control Room REST API base URL (used when demo_mode=false).",
    )
    jobs: List[AutomationAnywhereBotSpec] = Field(
        default_factory=list,
        description="Automation Anywhere Bots (fileIds) to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[AutomationAnywhereSourceTableSpec] = Field(
        default_factory=list,
        description="Tables populated by AA bots that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before bots run.",
    )
    group_name: str = Field(
        default="automation_anywhere_integration",
        description="Dagster asset group for the bot assets.",
    )

    automation_anywhere_user_env: str = Field(
        default="AUTOMATION_ANYWHERE_USER",
        description="Env var holding the Control Room REST username.",
    )
    automation_anywhere_password_env: str = Field(
        default="AUTOMATION_ANYWHERE_PASSWORD",
        description="Env var holding the Control Room REST password (or apiKey).",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between AA execution status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")
    log_retrieval: bool = Field(default=True, description="Retrieve execution logs on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Automation Anywhere vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.automation_anywhere_user_env
        password_env = self.automation_anywhere_password_env
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

        for bot in self.jobs:
            def _make_asset(
                _bot=bot,
                _endpoint=endpoint,
                _demo=demo_mode,
                _group=group,
                _upstream=resolved_upstream,
            ):
                @dg.asset(
                    name=_bot.asset_name,
                    kinds={"python", "automation-anywhere", "rpa"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_bot.description or (
                        f"Automation Anywhere Bot: fileId={_bot.file_id}. "
                        f"Deploys to workspace {_bot.workspace or 'Public'} "
                        f"(device pool: {_bot.device_pool_id or 'default'}), "
                        f"polls for completion, retrieves logs."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "automation_anywhere",
                        "integration_pattern": "dagster-orchestrates-automation-anywhere",
                        "scheduler_owner": "AutomationAnywhere",
                        "aa_file_id": _bot.file_id,
                        "aa_workspace": _bot.workspace,
                        "aa_application": _bot.application,
                        "aa_device_pool_id": _bot.device_pool_id,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_automation_anywhere(context, _bot, _endpoint)
                    else:
                        result = _execute_automation_anywhere(
                            context, _bot, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, log_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "deployment_id": str(result.get("deployment_id", "N/A")),
                        "execution_id": str(result.get("execution_id", "N/A")),
                        "status": result.get("status", "N/A"),
                        "partition_key": result.get("partition_key", "N/A"),
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
                    kinds={"automation-anywhere", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table populated by Automation Anywhere bot: {_table.table_name}. "
                        f"Data is loaded by AA bot executions."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "automation_anywhere_managed",
                        "table_name": _table.table_name,
                        "managed_by": "AutomationAnywhere",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Automation Anywhere (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown AA bot'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="automation_anywhere_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Automation Anywhere for deployments Dagster didn't trigger.",
        )
        def automation_anywhere_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                execution_id = int(uuid.uuid4().int % 10_000_000)
                context.log.info(f"[DETECTED] External Automation Anywhere deployment")
                context.log.info(f"  Triggered by: AA schedule / manual deploy")
                context.log.info(f"  Status: COMPLETED")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  executionId: {execution_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "automation_anywhere_bot"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_execution_id": dg.MetadataValue.text(str(execution_id)),
                        "platform": dg.MetadataValue.text("automation_anywhere"),
                        "triggered_by": dg.MetadataValue.text("AA schedule"),
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
                        token = _aa_login(_ep, user, pw)
                        headers = _aa_headers(token)
                        since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3600))
                        list_body = {
                            "filter": {"operator": "gte", "field": "createdOn", "value": since},
                            "sort": [{"field": "createdOn", "direction": "desc"}],
                            "page": {"offset": 0, "length": 20},
                        }
                        resp = requests.post(
                            f"{_ep}/v3/activity/list",
                            json=list_body, headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for entry in resp.json().get("list", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"aa_{str(entry.get('fileId', 'unknown'))}"),
                                metadata={
                                    "external_execution_id": dg.MetadataValue.text(str(entry.get("id", "unknown"))),
                                    "status": dg.MetadataValue.text(entry.get("status", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"Automation Anywhere monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={
            "file_id": dg.Field(int, is_required=False, default_value=0),
            "device_pool_id": dg.Field(int, is_required=False, default_value=0),
        })
        def redeploy_automation_anywhere_bot(context: dg.OpExecutionContext):
            """Redeploy a bot. Config: {file_id, device_pool_id}."""
            file_id = context.op_config.get("file_id", 0)
            device_pool_id = context.op_config.get("device_pool_id", 0)
            if _demo:
                context.log.info(f"[REDEPLOY] POST {_ep}/v3/automations/deploy")
                context.log.info(f"  Payload: {{fileId: {file_id}, poolIds: [{device_pool_id}]}}")
                context.log.info(f"  Response: 200 OK — bot redeployed")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _aa_login(_ep, u or "", p or "")
                headers = _aa_headers(token)
                _req.post(
                    f"{_ep}/v3/automations/deploy",
                    json={
                        "fileId": file_id,
                        "poolIds": [device_pool_id] if device_pool_id else [],
                        "overrideDefaultDevice": False,
                    },
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[REDEPLOY] Bot fileId={file_id} redeployed")

        @dg.op(config_schema={"execution_id": dg.Field(int, is_required=False, default_value=0)})
        def pause_automation_anywhere_execution(context: dg.OpExecutionContext):
            """Pause a running execution. Config: {execution_id}."""
            execution_id = context.op_config.get("execution_id", 0)
            if _demo:
                context.log.info(f"[PAUSE] POST {_ep}/v3/activity/execution/{execution_id}/pause")
                context.log.info(f"  Response: 200 OK — execution paused")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _aa_login(_ep, u or "", p or "")
                headers = _aa_headers(token)
                _req.post(
                    f"{_ep}/v3/activity/execution/{execution_id}/pause",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"execution_id": dg.Field(int, is_required=False, default_value=0)})
        def resume_automation_anywhere_execution(context: dg.OpExecutionContext):
            """Resume a paused execution. Config: {execution_id}."""
            execution_id = context.op_config.get("execution_id", 0)
            if _demo:
                context.log.info(f"[RESUME] POST {_ep}/v3/activity/execution/{execution_id}/resume")
                context.log.info(f"  Response: 200 OK — execution resumed")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _aa_login(_ep, u or "", p or "")
                headers = _aa_headers(token)
                _req.post(
                    f"{_ep}/v3/activity/execution/{execution_id}/resume",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"execution_id": dg.Field(int, is_required=False, default_value=0)})
        def stop_automation_anywhere_execution(context: dg.OpExecutionContext):
            """Stop a running execution. Config: {execution_id}."""
            execution_id = context.op_config.get("execution_id", 0)
            if _demo:
                context.log.info(f"[STOP] POST {_ep}/v3/activity/execution/{execution_id}/stop")
                context.log.info(f"  Response: 200 OK — execution stopped")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _aa_login(_ep, u or "", p or "")
                headers = _aa_headers(token)
                _req.post(
                    f"{_ep}/v3/activity/execution/{execution_id}/stop",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_automation_anywhere_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Automation Anywhere's actual state."""
            if _demo:
                context.log.info(f"[RECON] POST {_ep}/v3/activity/list (filter: createdOn >= now-1h)")
                context.log.info(f"  Automation Anywhere: 48 executions — 40 COMPLETED, 3 FAILED, 4 RUNNING, 1 QUEUED")
                context.log.info(f"  Dagster: materializations for 38 of 40 COMPLETED executions")
                context.log.info(f"  DRIFT: 2 executions completed in AA but not in Dagster")
                context.log.info(f"  ALERT: 3 executions in FAILED — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                token = _aa_login(_ep, u or "", p or "")
                headers = _aa_headers(token)
                since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3600))
                list_body = {
                    "filter": {"operator": "gte", "field": "createdOn", "value": since},
                    "sort": [{"field": "createdOn", "direction": "desc"}],
                    "page": {"offset": 0, "length": 200},
                }
                resp = _req.post(
                    f"{_ep}/v3/activity/list",
                    json=list_body, headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                executions = resp.json().get("list", [])
                by_status: dict = {}
                for ex in executions:
                    by_status.setdefault(ex.get("status", "?"), []).append(ex)
                context.log.info(f"[RECON] Automation Anywhere: {len(executions)} executions")
                for st, exs in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(exs)}")
                for ex in by_status.get("FAILED", []):
                    context.log.warning(f"  ALERT: fileId={ex.get('fileId')} — FAILED")

        @dg.job(description="Redeploy an Automation Anywhere bot.")
        def automation_anywhere_redeploy_bot():
            redeploy_automation_anywhere_bot()

        @dg.job(description="Pause a running Automation Anywhere execution.")
        def automation_anywhere_pause_execution():
            pause_automation_anywhere_execution()

        @dg.job(description="Resume a paused Automation Anywhere execution.")
        def automation_anywhere_resume_execution():
            resume_automation_anywhere_execution()

        @dg.job(description="Stop a running Automation Anywhere execution.")
        def automation_anywhere_stop_execution():
            stop_automation_anywhere_execution()

        @dg.job(description="Reconcile Dagster state with Automation Anywhere — detect drift.")
        def automation_anywhere_reconciliation():
            reconcile_automation_anywhere_state()

        recon_schedule = dg.ScheduleDefinition(
            name="automation_anywhere_reconciliation_schedule",
            job=automation_anywhere_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="automation_anywhere_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Automation Anywhere-initiated pipeline triggers. In production, "
                "an AA bot's callbackInfo URL posts to a Dagster webhook / GraphQL launchRun "
                "mutation on terminal status."
            ),
        )
        def automation_anywhere_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  Automation Anywhere trigger detected (tick {tick})")
                context.log.info(f"  Source: AA bot callbackInfo (post-execution)")
                context.log.info(f"  Action: In production, AA calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[automation_anywhere_external_monitor, automation_anywhere_inbound_trigger],
            jobs=[
                automation_anywhere_redeploy_bot,
                automation_anywhere_pause_execution,
                automation_anywhere_resume_execution,
                automation_anywhere_stop_execution,
                automation_anywhere_reconciliation,
            ],
            schedules=[recon_schedule],
        )
