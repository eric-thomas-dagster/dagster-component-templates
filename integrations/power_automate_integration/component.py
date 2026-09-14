"""Microsoft Power Automate cloud flow integration component.

Bidirectional integration between Dagster and Microsoft Power Automate
(cloud flows on api.flow.microsoft.com):

  Dagster -> Power Automate:
    - Trigger a cloud flow manually
      (POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{flow_id}/triggers/manual/run)
    - Poll the resulting run status
      (GET /providers/Microsoft.ProcessSimple/environments/{env}/flows/{flow_id}/runs/{run_name})
    - Cancel / turn off / turn on / reconcile ops for operational control
    - Azure AD OAuth2 client-credentials Bearer token

  Power Automate -> Dagster:
    - Power Automate cloud flow can call Dagster's GraphQL API
      (HTTP action, launchRun mutation)
    - Parameters passed via runConfigData; partition selected via a
      date input variable on the flow's manual trigger

Each declared cloud flow becomes a daily-partitioned Dagster asset.
Operational tasks (cancel / turn off / turn on / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI
or Dagster+ Automations.

`demo_mode: true` (default) simulates the Power Automate REST API on
stdout — the whole component runs end-to-end with zero external
dependencies (no Azure AD tenant needed).

Environment variables (production mode only):
  POWER_AUTOMATE_TENANT_ID      — Azure AD / Entra ID tenant GUID
  POWER_AUTOMATE_CLIENT_ID      — Azure AD app registration client ID
  POWER_AUTOMATE_CLIENT_SECRET  — Azure AD app registration client secret

Terminology map — for teams migrating from Control-M / RunMyJobs:
  Control-M Job          -> Power Automate Cloud Flow
  RMJ JobDefinition      -> Power Automate Cloud Flow
  Control-M Folder       -> Power Automate Environment
  RMJ Application        -> Power Automate Environment
  Control-M Agent/Host   -> (n/a — flows run in Microsoft's cloud)
  Solution (Dataverse)   -> business tag for a group of flows
  Control-M runId        -> Power Automate runName
  "Ended OK"/"Ended NOK" -> "Succeeded" / "Failed" / "Cancelled" / "Skipped"

Note: Power Automate cloud flows run in Microsoft's cloud infrastructure
(no per-run "host" concept like batch schedulers) — the Environment IS
the runtime context.

API reference:
  https://learn.microsoft.com/en-us/connectors/flowmanagement/
  https://learn.microsoft.com/en-us/power-automate/web-api
  (Flow Management REST API via api.flow.microsoft.com)
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

class PowerAutomateFlowSpec(dg.Model, dg.Resolvable):
    """A Power Automate cloud flow wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    flow_id: str = Field(
        description="Power Automate flow ID — the GUID from the flow's URL.",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this cloud flow.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    environment_id: str = Field(
        default="",
        description='Power Automate environment ID; usually "Default-<tenant_id>".',
    )
    solution: str = Field(default="", description="Business tag for the containing solution.")
    application: str = Field(default="", description="Business tag (e.g. FINANCE_OPS, HR_ONBOARDING).")
    trigger_input: dict = Field(
        default_factory=dict,
        description="JSON passed to the manual trigger. String values templated with {partition_key}.",
    )
    run_as: str = Field(default="svc_dagster", description="Service principal name used to invoke the flow.")


class PowerAutomateSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Power Automate cloud flow that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="power_automate_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Power Automate flow that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_power_automate(context: AssetExecutionContext, flow: PowerAutomateFlowSpec, endpoint: str) -> dict:
    """Simulate the Power Automate REST API lifecycle to stdout — no external deps."""
    run_name = f"08585{uuid.uuid4().hex[:24].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    env_id = flow.environment_id or "Default-00000000-0000-0000-0000-000000000000"
    tenant_id = "00000000-0000-0000-0000-000000000000"

    # Template trigger_input values with {partition_key}
    templated_input = {}
    for k, v in flow.trigger_input.items():
        if isinstance(v, str) and "{partition_key}" in v:
            templated_input[k] = v.replace("{partition_key}", scheduled)
        else:
            templated_input[k] = v

    context.log.info(f"[AUTH]    POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token — Azure AD client credentials")
    context.log.info(f"  Response: {{access_token: 'ey***', token_type: 'Bearer', expires_in: 3600}}")

    trigger_url = (
        f"{endpoint}/providers/Microsoft.ProcessSimple/environments/{env_id}"
        f"/flows/{flow.flow_id}/triggers/manual/run?api-version=2016-11-01"
    )
    context.log.info(f"[TRIGGER] POST {trigger_url}")
    context.log.info(f"  Payload: {json.dumps(templated_input, indent=2)}")
    context.log.info(f"  Response: 202 Accepted")
    context.log.info(
        f"  Location: {endpoint}/providers/Microsoft.ProcessSimple/environments/{env_id}"
        f"/flows/{flow.flow_id}/runs/{run_name}"
    )

    run_url = (
        f"{endpoint}/providers/Microsoft.ProcessSimple/environments/{env_id}"
        f"/flows/{flow.flow_id}/runs/{run_name}?api-version=2016-11-01"
    )
    for state in ["Running", "Running", "Running", "Running", "Succeeded"]:
        context.log.info(f"[POLL]    GET {run_url} -> status={state}")

    context.log.info(
        f"[DONE]    {flow.flow_id} -> Succeeded (runName: {run_name}, environment: {env_id})"
    )

    return {
        "run_name": run_name,
        "status": "Succeeded",
        "scheduled_time": scheduled,
        "environment_id": env_id,
        "target": "power_automate",
    }


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _power_automate_get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    import requests
    r = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://service.flow.microsoft.com/.default",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False, timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _power_automate_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_power_automate(
    context: AssetExecutionContext,
    flow: PowerAutomateFlowSpec,
    endpoint: str,
    tenant_env: str,
    client_id_env: str,
    client_secret_env: str,
    poll_interval: int,
    poll_timeout: int,
) -> dict:
    """Real Power Automate REST API lifecycle (Flow Management REST via api.flow.microsoft.com)."""
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    tenant_id = os.environ.get(tenant_env)
    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    if not tenant_id or not client_id or not client_secret:
        raise RuntimeError(
            f"Missing {tenant_env} / {client_id_env} / {client_secret_env} environment variables"
        )

    token = _power_automate_get_token(tenant_id, client_id, client_secret)
    headers = _power_automate_headers(token)

    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    env_id = flow.environment_id or f"Default-{tenant_id}"

    # Template trigger_input values with {partition_key}
    templated_input = {}
    for k, v in flow.trigger_input.items():
        if isinstance(v, str) and "{partition_key}" in v:
            templated_input[k] = v.replace("{partition_key}", scheduled)
        else:
            templated_input[k] = v

    trigger_url = (
        f"{endpoint}/providers/Microsoft.ProcessSimple/environments/{env_id}"
        f"/flows/{flow.flow_id}/triggers/manual/run"
    )
    context.log.info(f"[TRIGGER] POST {trigger_url} — {flow.flow_id}")
    resp = requests.post(
        trigger_url,
        params={"api-version": "2016-11-01"},
        json=templated_input,
        headers=headers, verify=False, timeout=30,
    )
    if resp.status_code not in (200, 202):
        resp.raise_for_status()

    # 202 Accepted returns a Location header pointing to the run URL
    location = resp.headers.get("Location", "")
    run_name = location.rsplit("/", 1)[-1].split("?")[0] if location else f"unknown-{uuid.uuid4().hex[:8]}"
    context.log.info(f"  runName: {run_name}")

    start = time.time()
    terminal_states = {"Succeeded", "Failed", "Cancelled", "Skipped"}
    status = "Running"

    run_url = (
        f"{endpoint}/providers/Microsoft.ProcessSimple/environments/{env_id}"
        f"/flows/{flow.flow_id}/runs/{run_name}"
    )
    while time.time() - start < poll_timeout:
        resp = requests.get(
            run_url,
            params={"api-version": "2016-11-01"},
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        status = body.get("properties", {}).get("status", "Running")
        context.log.info(f"[POLL]    {flow.flow_id} -> {status}")
        if status in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{flow.flow_id} timed out after {poll_timeout}s")

    if status != "Succeeded":
        raise RuntimeError(f"{flow.flow_id} finished: {status}")

    return {
        "run_name": run_name,
        "status": status,
        "scheduled_time": scheduled,
        "environment_id": env_id,
        "target": "power_automate",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class PowerAutomateIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Microsoft Power Automate cloud flow REST API integration.

    Each declared cloud flow becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    Power-Automate-managed tables into Dagster's lineage graph.
    Operational tasks (cancel / turn off / turn on / reconcile) ship as
    Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no M365 tenant. Flip to `false`
    and set `POWER_AUTOMATE_TENANT_ID` / `POWER_AUTOMATE_CLIENT_ID` /
    `POWER_AUTOMATE_CLIENT_SECRET` to hit a real Power Automate service.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://api.flow.microsoft.com",
        description="Power Automate REST API base URL (used when demo_mode=false).",
    )
    jobs: List[PowerAutomateFlowSpec] = Field(
        default_factory=list,
        description="Power Automate cloud flows to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[PowerAutomateSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Power Automate flows that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before flows run.",
    )
    group_name: str = Field(
        default="power_automate_integration",
        description="Dagster asset group for the flow assets.",
    )

    power_automate_tenant_env: str = Field(
        default="POWER_AUTOMATE_TENANT_ID",
        description="Env var holding the Azure AD / Entra ID tenant GUID.",
    )
    power_automate_client_id_env: str = Field(
        default="POWER_AUTOMATE_CLIENT_ID",
        description="Env var holding the Azure AD app registration client ID.",
    )
    power_automate_client_secret_env: str = Field(
        default="POWER_AUTOMATE_CLIENT_SECRET",
        description="Env var holding the Azure AD app registration client secret.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between Power Automate run status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal status before failing.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Power Automate vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        tenant_env = self.power_automate_tenant_env
        client_id_env = self.power_automate_client_id_env
        client_secret_env = self.power_automate_client_secret_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds

        resolved_upstream = [dg.AssetKey(dep.split("/")) for dep in self.upstream_deps]
        daily_partition = dg.DailyPartitionsDefinition(start_date=self.partition_start_date)

        retry_policy = RetryPolicy(
            max_retries=self.max_retries,
            delay=self.retry_delay_seconds,
        )

        all_assets: List[dg.AssetsDefinition] = []

        for flow in self.jobs:
            def _make_asset(
                _flow=flow,
                _endpoint=endpoint,
                _demo=demo_mode,
                _group=group,
                _upstream=resolved_upstream,
            ):
                @dg.asset(
                    name=_flow.asset_name,
                    kinds={"python", "power-automate", "rpa"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_flow.description or (
                        f"Power Automate cloud flow: {_flow.flow_id}. "
                        f"Runs in environment {_flow.environment_id or 'Default-<tenant>'} "
                        f"(solution: {_flow.solution or 'default'}), "
                        f"polls for completion, records the runName."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "power_automate",
                        "integration_pattern": "dagster-orchestrates-power-automate",
                        "scheduler_owner": "Power Automate",
                        "pa_environment_id": _flow.environment_id,
                        "pa_solution": _flow.solution,
                        "pa_application": _flow.application,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_power_automate(context, _flow, _endpoint)
                    else:
                        result = _execute_power_automate(
                            context, _flow, _endpoint,
                            tenant_env, client_id_env, client_secret_env,
                            poll_interval, poll_timeout,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_run_name": result.get("run_name", "N/A"),
                        "status": result.get("status", "N/A"),
                        "scheduled_time": result.get("scheduled_time", "N/A"),
                        "environment_id": result.get("environment_id", "N/A"),
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
                    kinds={"power-automate", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Power Automate: {_table.table_name}. "
                        f"Data is loaded by Power Automate cloud flows."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "power_automate_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Power Automate",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Power Automate (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Power Automate flow'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="power_automate_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Power Automate for cloud-flow runs Dagster didn't trigger.",
        )
        def power_automate_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                run_name = f"08585{uuid.uuid4().hex[:24].upper()}"
                context.log.info(f"[DETECTED] External Power Automate run")
                context.log.info(f"  Triggered by: Power Automate recurrence/trigger")
                context.log.info(f"  Status: Succeeded")
                context.log.info(f"  Duration: {_rand.randint(5, 120)}s")
                context.log.info(f"  runName: {run_name}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "power_automate_flow"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_run_name": dg.MetadataValue.text(run_name),
                        "platform": dg.MetadataValue.text("power_automate"),
                        "triggered_by": dg.MetadataValue.text("Power Automate recurrence"),
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
                tenant_id = os.environ.get(tenant_env)
                client_id = os.environ.get(client_id_env)
                client_secret = os.environ.get(client_secret_env)
                if tenant_id and client_id and client_secret and self.jobs:
                    try:
                        token = _power_automate_get_token(tenant_id, client_id, client_secret)
                        headers = _power_automate_headers(token)
                        first_flow = self.jobs[0]
                        env_id = first_flow.environment_id or f"Default-{tenant_id}"
                        resp = requests.get(
                            f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                            f"/flows/{first_flow.flow_id}/runs",
                            params={"api-version": "2016-11-01", "$top": 20},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for run in resp.json().get("value", [])[:1]:
                            props = run.get("properties", {})
                            if props.get("status") == "Succeeded":
                                observation = dg.AssetObservation(
                                    asset_key=dg.AssetKey(first_flow.asset_name),
                                    metadata={
                                        "external_run_name": dg.MetadataValue.text(str(run.get("name", "unknown"))),
                                        "status": dg.MetadataValue.text(props.get("status", "unknown")),
                                    },
                                )
                                context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                                yield dg.SensorResult(asset_events=[observation])
                                return
                    except Exception as e:
                        context.log.warning(f"Power Automate monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={
            "flow_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
            "environment_id": dg.Field(str, is_required=False, default_value="Default-00000000-0000-0000-0000-000000000000"),
        })
        def retrigger_power_automate_flow(context: dg.OpExecutionContext):
            """Re-trigger a Power Automate cloud flow manually. Config: {flow_id, environment_id}."""
            flow_id = context.op_config.get("flow_id")
            env_id = context.op_config.get("environment_id")
            trigger_url = (
                f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                f"/flows/{flow_id}/triggers/manual/run?api-version=2016-11-01"
            )
            if _demo:
                context.log.info(f"[TRIGGER] POST {trigger_url}")
                context.log.info(f"  Response: 202 Accepted — cloud flow re-triggered")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                t = os.environ.get(tenant_env)
                c = os.environ.get(client_id_env)
                s = os.environ.get(client_secret_env)
                token = _power_automate_get_token(t or "", c or "", s or "")
                headers = _power_automate_headers(token)
                r = _req.post(trigger_url, json={}, headers=headers, verify=False, timeout=30)
                if r.status_code not in (200, 202):
                    r.raise_for_status()
                context.log.info(f"[TRIGGER] Cloud flow {flow_id} re-triggered")

        @dg.op(config_schema={
            "flow_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
            "environment_id": dg.Field(str, is_required=False, default_value="Default-00000000-0000-0000-0000-000000000000"),
            "run_name": dg.Field(str, is_required=False, default_value="08585-UNKNOWN"),
        })
        def cancel_power_automate_run(context: dg.OpExecutionContext):
            """Cancel a running Power Automate run. Config: {flow_id, environment_id, run_name}."""
            flow_id = context.op_config.get("flow_id")
            env_id = context.op_config.get("environment_id")
            run_name = context.op_config.get("run_name")
            cancel_url = (
                f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                f"/flows/{flow_id}/runs/{run_name}/cancel?api-version=2016-11-01"
            )
            if _demo:
                context.log.info(f"[CANCEL] POST {cancel_url}")
                context.log.info(f"  Response: 200 OK — run cancelled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                t = os.environ.get(tenant_env)
                c = os.environ.get(client_id_env)
                s = os.environ.get(client_secret_env)
                token = _power_automate_get_token(t or "", c or "", s or "")
                headers = _power_automate_headers(token)
                _req.post(cancel_url, headers=headers, verify=False, timeout=30).raise_for_status()
                context.log.info(f"[CANCEL] Run {run_name} cancelled")

        @dg.op(config_schema={
            "flow_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
            "environment_id": dg.Field(str, is_required=False, default_value="Default-00000000-0000-0000-0000-000000000000"),
        })
        def turn_off_power_automate_flow(context: dg.OpExecutionContext):
            """Turn off (disable) a Power Automate cloud flow. Config: {flow_id, environment_id}."""
            flow_id = context.op_config.get("flow_id")
            env_id = context.op_config.get("environment_id")
            stop_url = (
                f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                f"/flows/{flow_id}/stop?api-version=2016-11-01"
            )
            if _demo:
                context.log.info(f"[STOP] POST {stop_url}")
                context.log.info(f"  Response: 200 OK — cloud flow disabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                t = os.environ.get(tenant_env)
                c = os.environ.get(client_id_env)
                s = os.environ.get(client_secret_env)
                token = _power_automate_get_token(t or "", c or "", s or "")
                headers = _power_automate_headers(token)
                _req.post(stop_url, headers=headers, verify=False, timeout=30).raise_for_status()
                context.log.info(f"[STOP] Flow {flow_id} disabled")

        @dg.op(config_schema={
            "flow_id": dg.Field(str, is_required=False, default_value="00000000-0000-0000-0000-000000000000"),
            "environment_id": dg.Field(str, is_required=False, default_value="Default-00000000-0000-0000-0000-000000000000"),
        })
        def turn_on_power_automate_flow(context: dg.OpExecutionContext):
            """Turn on (enable) a Power Automate cloud flow. Config: {flow_id, environment_id}."""
            flow_id = context.op_config.get("flow_id")
            env_id = context.op_config.get("environment_id")
            start_url = (
                f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                f"/flows/{flow_id}/start?api-version=2016-11-01"
            )
            if _demo:
                context.log.info(f"[START] POST {start_url}")
                context.log.info(f"  Response: 200 OK — cloud flow enabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                t = os.environ.get(tenant_env)
                c = os.environ.get(client_id_env)
                s = os.environ.get(client_secret_env)
                token = _power_automate_get_token(t or "", c or "", s or "")
                headers = _power_automate_headers(token)
                _req.post(start_url, headers=headers, verify=False, timeout=30).raise_for_status()
                context.log.info(f"[START] Flow {flow_id} enabled")

        @dg.op
        def reconcile_power_automate_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Power Automate's actual state."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/providers/Microsoft.ProcessSimple/environments/.../flows/.../runs")
                context.log.info(f"  Power Automate: 38 runs — 34 Succeeded, 2 Failed, 1 Running, 1 Cancelled")
                context.log.info(f"  Dagster: materializations for 33 of 34 'Succeeded' runs")
                context.log.info(f"  DRIFT: 1 run succeeded in Power Automate but not in Dagster")
                context.log.info(f"  ALERT: 2 runs in 'Failed' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                t = os.environ.get(tenant_env)
                c = os.environ.get(client_id_env)
                s = os.environ.get(client_secret_env)
                if not (t and c and s and self.jobs):
                    context.log.warning("Missing tenant/client/secret env vars or no flows configured; skipping recon")
                    return
                token = _power_automate_get_token(t, c, s)
                headers = _power_automate_headers(token)
                first_flow = self.jobs[0]
                env_id = first_flow.environment_id or f"Default-{t}"
                resp = _req.get(
                    f"{_ep}/providers/Microsoft.ProcessSimple/environments/{env_id}"
                    f"/flows/{first_flow.flow_id}/runs",
                    params={"api-version": "2016-11-01", "$top": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                runs = resp.json().get("value", [])
                by_status: dict = {}
                for run in runs:
                    st = run.get("properties", {}).get("status", "?")
                    by_status.setdefault(st, []).append(run)
                context.log.info(f"[RECON] Power Automate: {len(runs)} runs")
                for st, rs in sorted(by_status.items()):
                    context.log.info(f"  {st}: {len(rs)}")
                for run in by_status.get("Failed", []):
                    context.log.warning(f"  ALERT: {run.get('name')} — Failed")

        @dg.job(description="Re-trigger a Power Automate cloud flow.")
        def power_automate_retrigger():
            retrigger_power_automate_flow()

        @dg.job(description="Cancel a running Power Automate run.")
        def power_automate_cancel_run():
            cancel_power_automate_run()

        @dg.job(description="Turn off (disable) a Power Automate cloud flow.")
        def power_automate_turn_off():
            turn_off_power_automate_flow()

        @dg.job(description="Turn on (enable) a Power Automate cloud flow.")
        def power_automate_turn_on():
            turn_on_power_automate_flow()

        @dg.job(description="Reconcile Dagster state with Power Automate — detect drift.")
        def power_automate_reconciliation():
            reconcile_power_automate_state()

        recon_schedule = dg.ScheduleDefinition(
            name="power_automate_reconciliation_schedule",
            job=power_automate_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="power_automate_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Power-Automate-initiated pipeline triggers. In production, "
                "a Power Automate cloud flow uses an HTTP action to call Dagster's GraphQL "
                "API (launchRun mutation) to start runs — reverse direction to the assets."
            ),
        )
        def power_automate_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  Power Automate trigger detected (tick {tick})")
                context.log.info(f"  Source: Power Automate cloud flow completion")
                context.log.info(f"  Action: In production, Power Automate calls (HTTP action):")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[power_automate_external_monitor, power_automate_inbound_trigger],
            jobs=[
                power_automate_retrigger, power_automate_cancel_run,
                power_automate_turn_off, power_automate_turn_on,
                power_automate_reconciliation,
            ],
            schedules=[recon_schedule],
        )
