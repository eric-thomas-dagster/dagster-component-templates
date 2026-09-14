"""Jenkins integration component.

Bidirectional integration between Dagster and Jenkins for teams migrating
off Jenkins (or living alongside it during the transition):

  Dagster -> Jenkins:
    - Trigger a job via REST (POST /job/<name>/build or /buildWithParameters)
    - Poll the queue item then the build for terminal result
    - Retrieve consoleText on completion
    - Disable / enable / stop ops for operational control
    - HTTP Basic Auth using a Jenkins API token

  Jenkins -> Dagster:
    - Jenkins post-build step (curl / Groovy) can call Dagster's GraphQL
      launchRun mutation to fire a downstream pipeline

Each declared Jenkins job becomes a daily-partitioned Dagster asset.
Operational tasks (rebuild / disable / enable / stop / reconcile) ship as
Dagster jobs backed by ops so the customer can wire them into Dagster+
Automations, run them from the UI, or invoke them via GraphQL.

`demo_mode: true` (default) simulates the full REST API lifecycle on
stdout — the whole component runs end-to-end with zero external
dependencies. Flip to `false` and set `JENKINS_USER` / `JENKINS_API_TOKEN`
to hit a real Jenkins controller.

Environment variables (production mode only):
  JENKINS_USER       — Jenkins user (see http://<host>/user/<name>/configure)
  JENKINS_API_TOKEN  — API token generated from that user's configure page

API reference:
  https://www.jenkins.io/doc/book/using/remote-access-api/
  https://www.jenkins.io/doc/book/security/csrf-protection/
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

class JenkinsJobSpec(dg.Model, dg.Resolvable):
    """A Jenkins job wrapped as a daily-partitioned Dagster asset."""

    model_config = ConfigDict(extra="forbid")

    job_name: str = Field(
        description=(
            "Jenkins job name as it appears in the URL. For folder-nested jobs, "
            "use 'folder/subfolder/job' (component translates to /job/folder/job/subfolder/job/job)."
        ),
    )
    asset_name: str = Field(description="Dagster asset name that wraps this job.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    folder: str = Field(default="", description="Jenkins folder path (business tag; also inferred from job_name).")
    application: str = Field(default="", description="Business tag (e.g. CORE_BANKING).")
    node_label: str = Field(default="", description="Jenkins node label the build runs on (analog to Control-M host / RMJ queue).")
    parameters: dict = Field(
        default_factory=dict,
        description="Parameters passed to buildWithParameters (key/value pairs). Values templated with {partition_key}.",
    )
    run_as: str = Field(default="svc_dagster", description="Business tag for the OS user that owns this pipeline.")


class JenkinsSourceTableSpec(dg.Model, dg.Resolvable):
    """A table loaded by a Jenkins job that Dagster observes (not orchestrates)."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(description="Fully-qualified table name (SCHEMA.TABLE).")
    asset_name: str = Field(description="Dagster asset name representing the table.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    group_name: str = Field(default="jenkins_managed_data", description="Dagster asset group for this table.")
    produced_by: Optional[str] = Field(
        default=None,
        description="Asset name of the Jenkins job that loads this table (creates lineage).",
    )


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════

def _jenkins_headers(user: str, api_token: str, crumb: Optional[dict] = None) -> dict:
    token = base64.b64encode(f"{user}:{api_token}".encode()).decode()
    headers = {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }
    if crumb:
        headers[crumb["crumbRequestField"]] = crumb["crumb"]
    return headers


def _job_url_path(job_name: str) -> str:
    """Convert 'folder/subfolder/job' into '/job/folder/job/subfolder/job/job'."""
    parts = [p for p in job_name.split("/") if p]
    return "/" + "/".join(f"job/{p}" for p in parts)


# ═════════════════════════════════════════════════════════════════════
# Demo simulator
# ═════════════════════════════════════════════════════════════════════

def _simulate_jenkins(context: AssetExecutionContext, job: JenkinsJobSpec, endpoint: str) -> dict:
    """Simulate the full REST API lifecycle on stdout — zero external deps."""
    build_number = int(time.time()) % 10000
    partition = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    queue_id = uuid.uuid4().hex[:6].upper()
    fake_basic = base64.b64encode(b"svc_dagster:*****").decode()
    job_path = _job_url_path(job.job_name)

    context.log.info(f"[AUTH]   Authorization: Basic {fake_basic[:12]}... (HTTP Basic + API token)")
    context.log.info(f"[CRUMB]  GET {endpoint}/crumbIssuer/api/json")
    context.log.info(f"  Response: {{crumb: 'ab12cd34ef56', crumbRequestField: 'Jenkins-Crumb'}}")

    params_qs = {"PARTITION_KEY": partition, **{k: str(v).replace("{partition_key}", partition) for k, v in (job.parameters or {}).items()}}
    build_ep = "buildWithParameters" if job.parameters else "build"
    context.log.info(f"[TRIGGER] POST {endpoint}{job_path}/{build_ep}")
    if job.parameters:
        context.log.info(f"  Params: {json.dumps(params_qs)}")
    context.log.info(f"  Response: 201 Created")
    context.log.info(f"  Location: {endpoint}/queue/item/{queue_id}/")

    context.log.info(f"[QUEUE]  GET {endpoint}/queue/item/{queue_id}/api/json -> waiting")
    context.log.info(f"[QUEUE]  GET {endpoint}/queue/item/{queue_id}/api/json -> executable.number={build_number}")

    for building_state in [True, True, True, False]:
        result = None if building_state else "SUCCESS"
        context.log.info(
            f"[POLL]   GET {endpoint}{job_path}/{build_number}/api/json "
            f"-> building={str(building_state).lower()}, result={result}"
        )

    context.log.info(f"[CONSOLE] GET {endpoint}{job_path}/{build_number}/consoleText -> 512 lines")
    context.log.info(f"[DONE]   {job.job_name} -> SUCCESS (buildNumber: {build_number}, partition: {partition})")

    return {"build_number": build_number, "result": "SUCCESS", "partition": partition, "queue_id": queue_id, "target": "jenkins"}


# ═════════════════════════════════════════════════════════════════════
# Production executor
# ═════════════════════════════════════════════════════════════════════

def _execute_jenkins(
    context: AssetExecutionContext,
    job: JenkinsJobSpec,
    endpoint: str,
    user_env: str,
    token_env: str,
    poll_interval: int,
    poll_timeout: int,
    console_retrieval: bool,
) -> dict:
    """Real Jenkins REST API lifecycle."""
    import os
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    user = os.environ.get(user_env)
    api_token = os.environ.get(token_env)
    if not user or not api_token:
        raise RuntimeError(f"Missing {user_env} and/or {token_env} environment variables")

    partition = context.partition_key if context.has_partition_key else time.strftime("%Y-%m-%d")
    job_path = _job_url_path(job.job_name)

    # ── CSRF crumb (required for POST since Jenkins 2.222) ────────────
    crumb: Optional[dict] = None
    try:
        crumb_resp = requests.get(
            f"{endpoint}/crumbIssuer/api/json",
            headers=_jenkins_headers(user, api_token),
            verify=False, timeout=30,
        )
        if crumb_resp.ok:
            crumb = crumb_resp.json()
            context.log.info(f"[CRUMB]  Acquired {crumb['crumbRequestField']}")
    except Exception as e:
        context.log.warning(f"CSRF crumb fetch failed (may be optional on older Jenkins): {e}")

    headers = _jenkins_headers(user, api_token, crumb)

    # ── Trigger ───────────────────────────────────────────────────────
    params_qs = {
        "PARTITION_KEY": partition,
        **{k: str(v).replace("{partition_key}", partition) for k, v in (job.parameters or {}).items()},
    }
    build_ep = "buildWithParameters" if job.parameters else "build"
    context.log.info(f"[TRIGGER] POST {endpoint}{job_path}/{build_ep} — {job.job_name} (partition: {partition})")
    resp = requests.post(
        f"{endpoint}{job_path}/{build_ep}",
        params=params_qs if job.parameters else None,
        headers=headers, verify=False, timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Jenkins trigger failed: HTTP {resp.status_code} — {resp.text[:200]}")
    queue_url = resp.headers.get("Location")
    if not queue_url:
        raise RuntimeError("Jenkins response missing Location header — cannot track build")
    context.log.info(f"  Location: {queue_url}")

    # ── Wait for queue item to launch a build ─────────────────────────
    start = time.time()
    build_number: Optional[int] = None
    while time.time() - start < poll_timeout:
        qr = requests.get(f"{queue_url.rstrip('/')}/api/json", headers=headers, verify=False, timeout=30)
        if qr.ok:
            queue_data = qr.json()
            executable = queue_data.get("executable")
            if executable and executable.get("number") is not None:
                build_number = executable["number"]
                context.log.info(f"[QUEUE]  buildNumber: {build_number}")
                break
            context.log.info(f"[QUEUE]  {job.job_name} still queued (why: {queue_data.get('why', '?')})")
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_name} never left the Jenkins queue after {poll_timeout}s")

    # ── Poll build until terminal ─────────────────────────────────────
    result = None
    while time.time() - start < poll_timeout:
        br = requests.get(
            f"{endpoint}{job_path}/{build_number}/api/json",
            headers=headers, verify=False, timeout=30,
        )
        if br.ok:
            build_data = br.json()
            result = build_data.get("result")
            building = build_data.get("building", False)
            context.log.info(f"[POLL]   {job.job_name}#{build_number} -> building={building}, result={result}")
            if not building and result is not None:
                break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"{job.job_name}#{build_number} did not finish within {poll_timeout}s")

    if result != "SUCCESS":
        raise RuntimeError(f"{job.job_name}#{build_number} finished: {result}")

    # ── Console ──────────────────────────────────────────────────────
    output_lines = 0
    if console_retrieval:
        try:
            out_resp = requests.get(
                f"{endpoint}{job_path}/{build_number}/consoleText",
                headers=headers, verify=False, timeout=30,
            )
            if out_resp.ok:
                output_lines = len(out_resp.text.split("\n"))
                context.log.info(f"[CONSOLE] {output_lines} lines")
                for line in out_resp.text.split("\n")[:20]:
                    context.log.info(f"    {line}")
        except Exception as e:
            context.log.warning(f"consoleText retrieval failed: {e}")

    return {
        "build_number": build_number, "result": result, "partition": partition,
        "output_lines": output_lines, "target": "jenkins",
    }


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

class JenkinsIntegrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Jenkins REST API integration.

    Each declared Jenkins job becomes a daily-partitioned Dagster asset
    with a retry policy. Optional source-table declarations bring
    Jenkins-managed tables into Dagster's lineage graph. Operational
    tasks (rebuild / disable / enable / stop / reconcile) ship as Dagster
    jobs.

    `demo_mode: true` (default) simulates the REST API on stdout so the
    whole component runs end-to-end with no Jenkins controller. Flip to
    `false` and set `JENKINS_USER` / `JENKINS_API_TOKEN` to hit a real
    Jenkins.
    """

    demo_mode: bool = Field(
        default=True,
        description="Simulate the Jenkins REST API on stdout (no external calls).",
    )
    endpoint: str = Field(
        default="http://localhost:8080",
        description="Jenkins controller base URL (used when demo_mode=false). No trailing slash.",
    )
    jobs: List[JenkinsJobSpec] = Field(
        default_factory=list,
        description="Jenkins jobs to wrap as daily-partitioned Dagster assets.",
    )
    source_tables: List[JenkinsSourceTableSpec] = Field(
        default_factory=list,
        description="Tables loaded by Jenkins that Dagster observes (adds to lineage).",
    )
    upstream_deps: List[str] = Field(
        default_factory=list,
        description="Upstream asset keys (slash-separated for nested keys) that must complete before jobs run.",
    )
    group_name: str = Field(
        default="jenkins_integration",
        description="Dagster asset group for the job assets.",
    )

    jenkins_user_env: str = Field(
        default="JENKINS_USER",
        description="Env var holding the Jenkins REST username.",
    )
    jenkins_token_env: str = Field(
        default="JENKINS_API_TOKEN",
        description="Env var holding the Jenkins user's API token.",
    )

    poll_interval_seconds: int = Field(default=5, description="Seconds between Jenkins queue/build status polls.")
    poll_timeout_seconds: int = Field(default=3600, description="Total seconds to wait for terminal result before failing.")
    console_retrieval: bool = Field(default=True, description="Retrieve consoleText on completion.")

    max_retries: int = Field(default=2, description="Dagster-side asset retries on failure.")
    retry_delay_seconds: int = Field(default=60, description="Delay between Dagster asset retries.")

    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for the daily partitioned assets.",
    )

    reconciliation_cron: str = Field(
        default="0 * * * *",
        description="Cron for the Jenkins vs Dagster state reconciliation job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        endpoint = self.endpoint.rstrip("/")
        group = self.group_name
        user_env = self.jenkins_user_env
        token_env = self.jenkins_token_env
        poll_interval = self.poll_interval_seconds
        poll_timeout = self.poll_timeout_seconds
        console_retrieval = self.console_retrieval

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
                    kinds={"python", "jenkins"},
                    group_name=_group,
                    deps=_upstream,
                    partitions_def=daily_partition,
                    retry_policy=retry_policy,
                    description=_job.description or (
                        f"Jenkins job: {_job.job_name}. "
                        f"Triggers via REST, polls the build to terminal result, "
                        f"retrieves consoleText."
                    ),
                    metadata={
                        "tier": "orchestration",
                        "domain": "jenkins",
                        "integration_pattern": "dagster-orchestrates-jenkins",
                        "scheduler_owner": "Jenkins",
                        "jenkins_folder": _job.folder,
                        "jenkins_node_label": _job.node_label,
                        "deployment_mode": "hybrid",
                    },
                )
                def _asset_fn(context: AssetExecutionContext) -> None:
                    start_time = time.time()
                    if _demo:
                        result = _simulate_jenkins(context, _job, _endpoint)
                    else:
                        result = _execute_jenkins(
                            context, _job, _endpoint,
                            user_env, token_env,
                            poll_interval, poll_timeout, console_retrieval,
                        )
                    duration = round(time.time() - start_time, 2)
                    context.add_output_metadata({
                        "build_number": result.get("build_number", "N/A"),
                        "result": result.get("result", "N/A"),
                        "partition": result.get("partition", "N/A"),
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
                    kinds={"jenkins", "database"},
                    group_name=_table.group_name,
                    deps=_deps,
                    description=_table.description or (
                        f"Table managed by Jenkins: {_table.table_name}. "
                        f"Data is loaded by Jenkins jobs."
                    ),
                    metadata={
                        "tier": "bronze",
                        "domain": "jenkins_managed",
                        "table_name": _table.table_name,
                        "managed_by": "Jenkins",
                    },
                )
                def _source_fn(context: AssetExecutionContext) -> None:
                    context.log.info(f"[TABLE]  {_table.table_name}")
                    context.log.info(f"  Managed by: Jenkins (external)")
                    context.log.info(f"  Produced by: {_table.produced_by or 'unknown Jenkins job'}")
                    context.log.info(f"  This asset represents the output data, visible in Dagster lineage")

                return _source_fn

            source_assets.append(_make_source())

        _demo = demo_mode
        _ep = endpoint

        @dg.sensor(
            name="jenkins_external_execution_monitor",
            minimum_interval_seconds=60,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description="Monitors Jenkins for builds Dagster didn't trigger.",
        )
        def jenkins_external_monitor(context: dg.SensorEvaluationContext):
            import random as _rand
            last_cursor = context.cursor or "0|"
            parts = last_cursor.split("|")
            tick = int(parts[0]) + 1

            if _demo:
                build_number = _rand.randint(100, 999)
                context.log.info(f"[DETECTED] External Jenkins build")
                context.log.info(f"  Triggered by: Jenkins timer trigger / SCM poll")
                context.log.info(f"  Result: SUCCESS")
                context.log.info(f"  Duration: {_rand.randint(60, 600)}s")
                context.log.info(f"  Build number: {build_number}")
                context.log.info(f"  Dagster was NOT the orchestrator — recording observation")

                first_asset = self.jobs[0].asset_name if self.jobs else "jenkins_job"
                observation = dg.AssetObservation(
                    asset_key=dg.AssetKey(first_asset),
                    metadata={
                        "build_number": dg.MetadataValue.int(build_number),
                        "platform": dg.MetadataValue.text("jenkins"),
                        "triggered_by": dg.MetadataValue.text("Jenkins timer / SCM poll"),
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
                token = os.environ.get(token_env)
                if user and token and self.jobs:
                    try:
                        headers = _jenkins_headers(user, token)
                        # Sample the first configured job's recent builds
                        first_job = self.jobs[0]
                        path = _job_url_path(first_job.job_name)
                        resp = requests.get(
                            f"{_ep}{path}/api/json?tree=builds[number,result,timestamp]",
                            headers=headers, verify=False, timeout=30,
                        )
                        resp.raise_for_status()
                        for build in resp.json().get("builds", [])[:1]:
                            observation = dg.AssetObservation(
                                asset_key=dg.AssetKey(first_job.asset_name),
                                metadata={
                                    "build_number": dg.MetadataValue.int(build.get("number", -1)),
                                    "result": dg.MetadataValue.text(str(build.get("result", "unknown"))),
                                },
                            )
                            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                            yield dg.SensorResult(asset_events=[observation])
                            return
                    except Exception as e:
                        context.log.warning(f"Jenkins monitor failed: {e}")

            context.update_cursor(f"{tick}|{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        @dg.op(config_schema={"job_name": dg.Field(str, is_required=False, default_value="my_job")})
        def rebuild_jenkins_job(context: dg.OpExecutionContext):
            """Rebuild a Jenkins job. Config: {job_name}."""
            job_name = context.op_config.get("job_name", "my_job")
            path = _job_url_path(job_name)
            if _demo:
                context.log.info(f"[REBUILD] POST {_ep}{path}/build")
                context.log.info(f"  Response: 201 Created — job resubmitted")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, t = os.environ.get(user_env), os.environ.get(token_env)
                try:
                    crumb_r = _req.get(f"{_ep}/crumbIssuer/api/json", headers=_jenkins_headers(u or "", t or ""), verify=False, timeout=30)
                    crumb = crumb_r.json() if crumb_r.ok else None
                except Exception:
                    crumb = None
                _req.post(
                    f"{_ep}{path}/build",
                    headers=_jenkins_headers(u or "", t or "", crumb),
                    verify=False, timeout=30,
                ).raise_for_status()
                context.log.info(f"[REBUILD] {job_name} resubmitted")

        @dg.op(config_schema={"job_name": dg.Field(str, is_required=False, default_value="my_job")})
        def disable_jenkins_job(context: dg.OpExecutionContext):
            """Disable a Jenkins job (analog to Control-M hold). Config: {job_name}."""
            job_name = context.op_config.get("job_name", "my_job")
            path = _job_url_path(job_name)
            if _demo:
                context.log.info(f"[DISABLE] POST {_ep}{path}/disable — {job_name} paused")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, t = os.environ.get(user_env), os.environ.get(token_env)
                try:
                    crumb_r = _req.get(f"{_ep}/crumbIssuer/api/json", headers=_jenkins_headers(u or "", t or ""), verify=False, timeout=30)
                    crumb = crumb_r.json() if crumb_r.ok else None
                except Exception:
                    crumb = None
                _req.post(
                    f"{_ep}{path}/disable",
                    headers=_jenkins_headers(u or "", t or "", crumb),
                    verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={"job_name": dg.Field(str, is_required=False, default_value="my_job")})
        def enable_jenkins_job(context: dg.OpExecutionContext):
            """Re-enable a disabled Jenkins job. Config: {job_name}."""
            job_name = context.op_config.get("job_name", "my_job")
            path = _job_url_path(job_name)
            if _demo:
                context.log.info(f"[ENABLE]  POST {_ep}{path}/enable — {job_name} re-enabled")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, t = os.environ.get(user_env), os.environ.get(token_env)
                try:
                    crumb_r = _req.get(f"{_ep}/crumbIssuer/api/json", headers=_jenkins_headers(u or "", t or ""), verify=False, timeout=30)
                    crumb = crumb_r.json() if crumb_r.ok else None
                except Exception:
                    crumb = None
                _req.post(
                    f"{_ep}{path}/enable",
                    headers=_jenkins_headers(u or "", t or "", crumb),
                    verify=False, timeout=30,
                ).raise_for_status()

        @dg.op(config_schema={
            "job_name": dg.Field(str, is_required=False, default_value="my_job"),
            "build_number": dg.Field(int, is_required=False, default_value=1),
        })
        def stop_jenkins_build(context: dg.OpExecutionContext):
            """Stop a running Jenkins build. Config: {job_name, build_number}."""
            job_name = context.op_config.get("job_name", "my_job")
            build_number = context.op_config.get("build_number", 1)
            path = _job_url_path(job_name)
            if _demo:
                context.log.info(f"[STOP] POST {_ep}{path}/{build_number}/stop — build terminated")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, t = os.environ.get(user_env), os.environ.get(token_env)
                try:
                    crumb_r = _req.get(f"{_ep}/crumbIssuer/api/json", headers=_jenkins_headers(u or "", t or ""), verify=False, timeout=30)
                    crumb = crumb_r.json() if crumb_r.ok else None
                except Exception:
                    crumb = None
                _req.post(
                    f"{_ep}{path}/{build_number}/stop",
                    headers=_jenkins_headers(u or "", t or "", crumb),
                    verify=False, timeout=30,
                ).raise_for_status()

        @dg.op
        def reconcile_jenkins_state(context: dg.OpExecutionContext):
            """Compare Dagster's view with Jenkins' actual state — report drift."""
            if _demo:
                context.log.info(f"[RECON] GET {_ep}/api/json?tree=jobs[name,color,lastBuild[number,result,timestamp]]")
                context.log.info(f"  Jenkins: 24 jobs — 18 blue (SUCCESS), 3 red (FAILURE), 2 yellow (UNSTABLE), 1 disabled")
                context.log.info(f"  Dagster: materializations for 17 of 18 'SUCCESS' jobs")
                context.log.info(f"  DRIFT: 1 job succeeded in Jenkins but not tracked in Dagster")
                context.log.info(f"  ALERT: 3 jobs in FAILURE — manual review required")
            else:
                import os
                import requests as _req
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                u, t = os.environ.get(user_env), os.environ.get(token_env)
                resp = _req.get(
                    f"{_ep}/api/json?tree=jobs[name,color,lastBuild[number,result,timestamp]]",
                    headers=_jenkins_headers(u or "", t or ""), verify=False, timeout=30,
                )
                resp.raise_for_status()
                jobs = resp.json().get("jobs", [])
                by_color: dict = {}
                for jj in jobs:
                    by_color.setdefault(jj.get("color", "?"), []).append(jj)
                context.log.info(f"[RECON] Jenkins: {len(jobs)} jobs")
                for color, jjs in sorted(by_color.items()):
                    context.log.info(f"  {color}: {len(jjs)}")
                for jj in by_color.get("red", []):
                    context.log.warning(f"  ALERT: {jj.get('name')} — last build FAILURE")

        @dg.job(description="Rebuild a Jenkins job.")
        def jenkins_rebuild_job():
            rebuild_jenkins_job()

        @dg.job(description="Disable (hold) a Jenkins job.")
        def jenkins_disable_job():
            disable_jenkins_job()

        @dg.job(description="Re-enable a disabled Jenkins job.")
        def jenkins_enable_job():
            enable_jenkins_job()

        @dg.job(description="Stop a running Jenkins build.")
        def jenkins_stop_build():
            stop_jenkins_build()

        @dg.job(description="Reconcile Dagster state with Jenkins — detect drift.")
        def jenkins_reconciliation():
            reconcile_jenkins_state()

        recon_schedule = dg.ScheduleDefinition(
            name="jenkins_reconciliation_schedule",
            job=jenkins_reconciliation,
            cron_schedule=self.reconciliation_cron,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )

        @dg.sensor(
            name="jenkins_inbound_trigger",
            minimum_interval_seconds=300,
            default_status=dg.DefaultSensorStatus.STOPPED,
            description=(
                "Monitors for Jenkins-initiated pipeline triggers. In production, "
                "a Jenkins post-build step (curl / Groovy) calls Dagster's GraphQL "
                "launchRun mutation to start a downstream run."
            ),
        )
        def jenkins_inbound_trigger(context: dg.SensorEvaluationContext):
            last_cursor = int(context.cursor) if context.cursor else 0
            tick = last_cursor + 1
            if _demo and tick % 3 == 0:
                context.log.info(f"[INBOUND] Jenkins trigger detected (tick {tick})")
                context.log.info(f"  Source: Jenkins post-build step")
                context.log.info(f"  Action: In production, Jenkins pipeline calls:")
                context.log.info(f"    POST https://dagster.cloud/graphql")
                context.log.info(f"    mutation {{ launchRun(executionParams: {{ ... }}) }}")
                context.log.info(f"  This sensor confirms the inbound trigger was received")
            context.update_cursor(str(tick))

        return dg.Definitions(
            assets=[*all_assets, *source_assets],
            sensors=[jenkins_external_monitor, jenkins_inbound_trigger],
            jobs=[jenkins_rebuild_job, jenkins_disable_job, jenkins_enable_job, jenkins_stop_build, jenkins_reconciliation],
            schedules=[recon_schedule],
        )
