"""Fortra JAMS Scheduler integration component.

Bidirectional integration between Dagster and Fortra (formerly HelpSystems)
JAMS Scheduler:

  Dagster -> JAMS:
    - Submit a JAMS Job (POST /Jobs/{name}/Submit)
    - Poll the resulting Entry status (GET /Entries/{entryId})
    - Retrieve entry log (GET /Entries/{entryId}/Log)
    - Restart / hold / release / cancel ops for operational control
    - HTTP Basic Auth (username:password in Authorization header)

  JAMS -> Dagster:
    - JAMS Job can call Dagster's GraphQL API (launchRun mutation)
    - Parameters passed via runConfigData; partition selected via ODATE-analog

Each declared JAMS Job becomes a daily-partitioned Dagster asset.
Operational tasks (restart / hold / release / cancel / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into the UI or
Dagster+ Automations.

`demo_mode: true` (default) simulates the JAMS REST API on stdout — the
whole component runs end-to-end with zero external dependencies. JAMS
runs on Windows Server with no public Docker image, so the simulator is
the only zero-license way to smoke-test the whole surface.

Environment variables (production mode only):
  JAMS_USER      — REST API username
  JAMS_PASSWORD  — REST API password

Terminology map — for teams migrating from Control-M or RunMyJobs:
  Control-M Job          -> JAMS Job                 (RMJ: JobDefinition)
  Control-M Folder       -> JAMS Folder              (RMJ: Application)
  Control-M Agent/Host   -> JAMS Agent               (RMJ: Queue)
  Control-M ODATE        -> JAMS scheduledTime       (RMJ: scheduledTime)
  Control-M runId        -> JAMS Entry ID            (RMJ: processId)
  "Ended OK" / "Ended Not OK" -> "Completed" / "Failed"

API reference:
  https://docs.jamsscheduler.com/
  (exact REST paths vary by JAMS version — 7.x vs 6.x REST surfaces
  differ. Verify against your instance when flipping demo_mode to
  false — see README.)
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

class JAMSJobSpec(dg.Model, dg.Resolvable):
    """A JAMS Job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_name: str = Field(
        description="JAMS Job name (as registered in the JAMS scheduler).",
    )
    asset_name: str = Field(description="Dagster asset name that wraps this JAMS Job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    folder: str = Field(default="", description="JAMS Folder (analog to Control-M Folder / RMJ Application).")
    agent: str = Field(default="", description="JAMS Agent the job runs on (analog to Control-M host / RMJ Queue).")
    application: str = Field(default="", description="Business tag for the job (e.g. CORE_BANKING).")
    run_as: str = Field(default="svc_dagster", description="OS user JAMS runs the job as.")


class JAMSSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a JAMS Job that Dagster observes."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="jams_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the JAMS job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_jams(context: AssetExecutionContext, job: JAMSJobSpec, endpoint: str) -> dict:
    """Simulate the JAMS REST API lifecycle to stdout — no external deps."""
    entry_id = f"JAMS-{uuid.uuid4().hex[:8].upper()}"
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "Parameters": {
            "SCHEDULED_TIME": scheduled,
            "FOLDER": job.folder,
            "AGENT": job.agent,
            "APPLICATION": job.application,
        },
    }
    fake_basic = base64.b64encode(b"svc_dagster:******").decode()

    context.log.info(f"[AUTH]   Authorization: Basic {fake_basic[:12]}... (HTTP Basic)")
    context.log.info(f"[SUBMIT] POST {endpoint}/Jobs/{job.job_name}/Submit -> entryId={entry_id}")
    context.log.info(f"  Payload: {json.dumps(submit_payload, indent=2)}")

    for state in ["Queued", "Scheduled", "Executing", "Executing", "Completed"]:
        context.log.info(f"[POLL]   GET {endpoint}/Entries/{entry_id} -> State={state}")

    context.log.info(f"[LOG]    GET {endpoint}/Entries/{entry_id}/Log -> 487 lines")
    context.log.info(f"[DONE]   {job.job_name} -> Completed (entryId: {entry_id}, folder: {job.folder})")

    return {"entry_id": entry_id, "state": "Completed", "scheduled_time": scheduled, "target": "jams"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _jams_headers(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _execute_jams(
    context: AssetExecutionContext,
    job: JAMSJobSpec,
    endpoint: str,
    user_env: str,
    password_env: str,
    poll_interval: int,
    poll_timeout: int,
    log_retrieval: bool,
) -> dict:
    """Real JAMS REST API lifecycle.

    REST paths vary across JAMS versions (6.x vs 7.x); the paths below
    target the modern JSON REST surface at /jams/rest/api. If your
    JAMS instance uses a different prefix, override `endpoint` to
    include the full prefix. Some deployments also require a token
    exchange via /Authentication before Basic auth is accepted.
    """
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not user or not password:
        raise RuntimeError(f"Missing {user_env} and/or {password_env} environment variables")

    headers = _jams_headers(user, password)
    scheduled = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")

    submit_payload = {
        "Parameters": {
            "SCHEDULED_TIME": scheduled,
            "FOLDER": job.folder,
            "AGENT": job.agent,
            "APPLICATION": job.application,
        },
    }
    context.log.info(f"[SUBMIT] POST {endpoint}/Jobs/{job.job_name}/Submit")
    resp = requests.post(
        f"{endpoint}/Jobs/{job.job_name}/Submit",
        json=submit_payload, headers=headers, verify=False, timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    entry_id = (
        (payload.get("Entry") or {}).get("ID")
        or payload.get("EntryID")
        or payload.get("entryId")
        or "unknown"
    )
    context.log.info(f"  entryId: {entry_id}")

    start = time.time()
    terminal_states = {"Completed", "Failed", "Cancelled"}
    state = "UNKNOWN"

    while time.time() - start < poll_timeout:
        resp = requests.get(
            f"{endpoint}/Entries/{entry_id}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()
        state = resp.json().get("State", "UNKNOWN")
        context.log.info(f"[POLL]   {job.job_name} -> {state}")
        if state in terminal_states:
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_name} timed out after {poll_timeout}s")

    if state != "Completed":
        raise RuntimeError(f"{job.job_name} finished: {state}")

    log_lines = 0
    if log_retrieval:
        try:
            log_resp = requests.get(
                f"{endpoint}/Entries/{entry_id}/Log",
                headers=headers, verify=False, timeout=30,
            )
            if log_resp.ok:
                log_lines = len(log_resp.text.split("\n"))
                context.log.info(f"[LOG]    {log_lines} lines")
                for line in log_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"log retrieval failed: {e}")

    return {"entry_id": entry_id, "state": state, "scheduled_time": scheduled, "log_lines": log_lines, "target": "jams"}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class JAMSIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fortra JAMS Scheduler REST API integration.

    Each declared JAMS Job becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    JAMS-managed tables into Dagster's lineage graph. Operational
    tasks (restart / hold / release / cancel / reconcile) ship as
    Dagster jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no JAMS endpoint. Flip to
    `false` and set `JAMS_USER` / `JAMS_PASSWORD` to hit a real
    JAMS instance.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="https://jams.internal/jams/rest/api",
        description="JAMS REST API base URL (used when demo_mode=false).",
    )
    jobs: List[JAMSJobSpec] = Field(
        default_factory=list,
        description="JAMS Jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[JAMSSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by JAMS that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="jams_integration",
        description="Dagster asset group for the job assets.",
    )

    jams_user_env: str = Field(
        default="JAMS_USER",
        description="Env var holding the JAMS REST username.",
    )
    jams_password_env: str = Field(
        default="JAMS_PASSWORD",
        description="Env var holding the JAMS REST password.",
    )

    poll_interval_seconds: int = Field(default=10, description="Seconds between JAMS entry status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal state before failing.")
    log_retrieval: bool = Field(default=True, description="Retrieve entry log on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the JAMS vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint
        group = self.group_name
        user_env = self.jams_user_env
        password_env = self.jams_password_env
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
                    kinds={"python", "jams"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"JAMS Job: {_job.job_name}. "
                        f"Submits to folder {_job.folder or 'default'} (agent: {_job.agent or 'default'}), "
                        f"polls for completion, retrieves log."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "jams",
                        "integration_pattern": "dagster-orchestrates-jams",
                        "scheduler_owner": "JAMS",
                        "jams_folder": _job.folder,
                        "jams_agent": _job.agent,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_jams(context, _job, _endpoint)
                    else:
                        result = _execute_jams(
                            context, _job, _endpoint,
                            user_env, password_env,
                            poll_interval, poll_timeout, log_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "external_entry_id": result.get("entry_id", "N/A"),
                        "state": result.get("state", "N/A"),
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
                    kinds={"jams", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by JAMS: {_table.table_name}. "
                        f"Data is loaded by JAMS jobs."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "jams_managed",
                        "table_name": _table.table_name,
                        "managed_by": "JAMS",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: JAMS (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown JAMS job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="jams_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors JAMS for entry completions Dagster didn't trigger.",
        )
        def jams_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                entry_id = f"ext-{uuid.uuid4().hex[:8]}"
                context.log.info(f"[DETECTED] External JAMS entry")
                context.log.info(f"  Triggered by: JAMS schedule")
                context.log.info(f"  State: Completed")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  entryId: {entry_id}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "jams_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "external_entry_id": dg.MetadataValue.text(entry_id),
                        "platform": dg.MetadataValue.text("jams"),
                        "triggered_by": dg.MetadataValue.text("JAMS schedule"),
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
                        headers = _jams_headers(user, pw)
                        resp = requests.get(
                            f"{_ep}/Entries",
                            params={"state": "Completed", "pageSize": 20, "lastRunAfter": "1h"},
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for e in resp.json().get("Entries", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(f"jams_{(e.get('JobName') or 'unknown').lower().replace(' ', '_')}"),
                                metadata={
                                    "external_entry_id": dg.MetadataValue.text(str(e.get("ID", "unknown"))),
                                    "state": dg.MetadataValue.text(e.get("State", "unknown")),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"JAMS monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"entry_id": dg.Field(str, is_required=False, default_value="JAMS-UNKNOWN")})
        def restart_jams_entry(context: dg.OpExecutionContext):
            """Restart a failed JAMS entry. Config: {entry_id: "JAMS-ABC"}."""
            entry_id = context.op_config.get("entry_id", "JAMS-UNKNOWN")
            if _demo:
                context.log.info(f"[RESTART] POST {_ep}/Entries/{entry_id}/Restart")
                context.log.info(f"  Response: 200 OK — entry resubmitted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _jams_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Entries/{entry_id}/Restart",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[RESTART] Entry {entry_id} resubmitted")

        @dg.op(config_schema={"entry_id": dg.Field(str, is_required=False, default_value="JAMS-UNKNOWN")})
        def hold_jams_entry(context: dg.OpExecutionContext):
            """Hold a JAMS entry. Config: {entry_id}."""
            entry_id = context.op_config.get("entry_id", "JAMS-UNKNOWN")
            if _demo:
                context.log.info(f"[HOLD] POST {_ep}/Entries/{entry_id}/Hold")
                context.log.info(f"  Response: 200 OK — entry held")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _jams_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Entries/{entry_id}/Hold",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"entry_id": dg.Field(str, is_required=False, default_value="JAMS-UNKNOWN")})
        def release_jams_entry(context: dg.OpExecutionContext):
            """Release a held JAMS entry. Config: {entry_id}."""
            entry_id = context.op_config.get("entry_id", "JAMS-UNKNOWN")
            if _demo:
                context.log.info(f"[RELEASE] POST {_ep}/Entries/{entry_id}/Release")
                context.log.info(f"  Response: 200 OK — entry released")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _jams_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Entries/{entry_id}/Release",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"entry_id": dg.Field(str, is_required=False, default_value="JAMS-UNKNOWN")})
        def cancel_jams_entry(context: dg.OpExecutionContext):
            """Cancel a running JAMS entry. Config: {entry_id}."""
            entry_id = context.op_config.get("entry_id", "JAMS-UNKNOWN")
            if _demo:
                context.log.info(f"[CANCEL] POST {_ep}/Entries/{entry_id}/Cancel")
                context.log.info(f"  Response: 200 OK — entry cancelled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _jams_headers(u or "", p or "")
                _req.post(
                    f"{_ep}/Entries/{entry_id}/Cancel",
                    headers=headers, verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_jams_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with JAMS' actual state."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/Entries?state=Completed,Failed&lastRunAfter=1h&pageSize=200")
                context.log.info(f"  JAMS: 48 entries — 41 Completed, 3 Failed, 3 Executing, 1 Held")
                context.log.info(f"  Dagster: materializations for 39 of 41 'Completed' entries")
                context.log.info(f"  DRIFT: 2 entries completed in JAMS but not in Dagster")
                context.log.info(f"  ALERT: 3 entries in 'Failed' — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, p = os.environ.get(user_env), os.environ.get(password_env)
                headers = _jams_headers(u or "", p or "")
                resp = _req.get(
                    f"{_ep}/Entries",
                    params={"state": "Completed,Failed", "lastRunAfter": "1h", "pageSize": 200},
                    headers=headers, verify=False, timeout=30,
                )
                resp.raise_for_status()
                entries = resp.json().get("Entries", [])
                by_state: dict = {}
                for en in entries:
                    by_state.setdefault(en.get("State", "?"), []).append(en)
                context.log.info(f"[RECON] JAMS: {len(entries)} entries")
                for st, ens in sorted(by_state.items()):
                    context.log.info(f"  {st}: {len(ens)}")
                for en in by_state.get("Failed", []):
                    context.log.warning(f"  ALERT: {en.get('JobName')} — Failed")

        @dg.job(description="Restart a failed JAMS entry.")
        def jams_restart_entry():
            restart_jams_entry()

        @dg.job(description="Hold a JAMS entry.")
        def jams_hold_entry():
            hold_jams_entry()

        @dg.job(description="Release a held JAMS entry.")
        def jams_release_entry():
            release_jams_entry()

        @dg.job(description="Cancel a running JAMS entry.")
        def jams_cancel_entry():
            cancel_jams_entry()

        @dg.job(description="Reconcile Dagster state with JAMS — detect drift.")
        def jams_reconciliation():
            reconcile_jams_state()

        recon_schedule = dg.ScheduleDefinition(
            name="jams_reconciliation_schedule",
            job=jams_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="jams_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for JAMS-initiated pipeline triggers. In production, "
                "a JAMS job calls Dagster's GraphQL API (launchRun mutation) to "
                "start runs after a scheduled entry completes."
            ),
        )
        def jams_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND]  JAMS trigger detected (tick {tick})")
                context.log.info(f"  Source: JAMS scheduled entry completion")
                context.log.info(f"  Action: In production, JAMS calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[jams_external_monitor, jams_inbound_trigger],
            jobs=[
                jams_restart_entry, jams_hold_entry,
                jams_release_entry, jams_cancel_entry,
                jams_reconciliation,
            ],
            schedules=[recon_schedule],
        )
