"""MeltanoSyncTriggerJobComponent.

Trigger a Meltano Cloud pipeline run via Meltano Cloud's REST API and
(optionally) poll it to completion.

Meltano itself is open-source and self-hosted by default -- it's run via its
own `meltano` CLI/orchestrator, not a single global SaaS API, so there is no
generic "remote-trigger a self-hosted Meltano project" surface to wrap here
(a self-hosted install would need `subprocess`-invoking the CLI on whatever
host it's installed on, which this component does not assume). Meltano
*Cloud* (meltano.com's managed, hosted offering) is different: it exposes a
real, documented REST API at `https://app.meltano.com/api` with Bearer-token
auth, a `POST /pipelines/{pipeline-id}/jobs` endpoint to start a run, and a
`GET /jobs/{job-id}` endpoint to poll it (status one of QUEUED / RUNNING /
COMPLETE / ERROR / STOPPED), plus `PUT /jobs/{job-id}/stopped` to cancel.
That's the real, triggerable surface this component wraps -- confirmed
against https://docs.meltano.com/reference/cloud/api/resources/jobs/ and
.../pipelines (2026-10). Meltano Cloud's own docs also note only a single
pipeline can run at a time per project -- a 4xx from the trigger call may
mean another pipeline run is already in flight, not a config error.

Logic lives in module-level functions (not inlined in the op closure) so it
can be unit-tested directly, same pattern as castordoc_export_job.
"""

import time
from typing import Any, Dict, Optional

import dagster as dg
import requests
from pydantic import Field

_TERMINAL_STATUSES = {"COMPLETE", "ERROR", "STOPPED"}


def _auth_headers(token_env: str) -> Dict[str, str]:
    import os

    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    return {"Authorization": f"Bearer {token}"}


def _trigger_pipeline_job(base_url: str, pipeline_id: str, headers: Dict[str, str]) -> Dict[str, Any]:
    r = requests.post(f"{base_url}/pipelines/{pipeline_id}/jobs", headers=headers, timeout=60)
    if r.status_code >= 300:
        raise Exception(f"meltano cloud trigger failed: {r.status_code} {r.text[:200]}")
    return r.json()


def _get_job(base_url: str, job_id: str, headers: Dict[str, str]) -> Dict[str, Any]:
    r = requests.get(f"{base_url}/jobs/{job_id}", headers=headers, timeout=30)
    if r.status_code >= 300:
        raise Exception(f"meltano cloud job lookup failed: {r.status_code} {r.text[:200]}")
    return r.json()


def _stop_job(base_url: str, job_id: str, headers: Dict[str, str]) -> None:
    try:
        requests.put(f"{base_url}/jobs/{job_id}/stopped", headers=headers, timeout=30)
    except Exception:
        pass


def _poll_until_terminal(
    base_url: str,
    job_id: str,
    headers: Dict[str, str],
    poll_interval_seconds: int,
    timeout_seconds: int,
    log,
    cancel_on_timeout: bool = False,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        job = _get_job(base_url, job_id, headers)
        status = job.get("status")
        log.info(f"poll: job={job_id} status={status}")
        if status in _TERMINAL_STATUSES:
            if status != "COMPLETE":
                raise Exception(
                    f"meltano cloud job {job_id} ended with status={status} exitCode={job.get('exitCode')}"
                )
            return job
        time.sleep(poll_interval_seconds)
    if cancel_on_timeout:
        _stop_job(base_url, job_id, headers)
    raise Exception(f"timed out waiting for meltano cloud job {job_id}")


class MeltanoSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Meltano Cloud pipeline run via REST API -- fire-and-forget or wait for completion."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    meltano_cloud_api_url: str = Field(
        default="https://app.meltano.com/api",
        description="Meltano Cloud API base URL",
    )
    pipeline_id: str = Field(description="Meltano Cloud pipeline UUID to run (see `meltano cloud pipeline list` or the Cloud UI)")
    api_token_env: str = Field(
        default="MELTANO_CLOUD_API_TOKEN",
        description="Env var holding the Meltano Cloud API bearer token (create one via the Cloud UI's API Keys page or `POST /api/apikeys`)",
    )
    wait_for_completion: bool = Field(default=False, description="Poll the triggered job until COMPLETE/ERROR/STOPPED")
    poll_interval_seconds: int = Field(default=15, description="Seconds between polls when wait_for_completion is true")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait before giving up")
    cancel_on_timeout: bool = Field(
        default=False,
        description="If the timeout is reached, PUT /jobs/{job-id}/stopped to cancel the still-running Meltano Cloud job before raising",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            headers = _auth_headers(self.api_token_env)
            job = _trigger_pipeline_job(self.meltano_cloud_api_url, self.pipeline_id, headers)
            job_id = job.get("id")
            context.log.info(f"triggered meltano cloud pipeline {self.pipeline_id}: job_id={job_id} status={job.get('status')}")
            if self.wait_for_completion and job_id:
                final = _poll_until_terminal(
                    self.meltano_cloud_api_url,
                    job_id,
                    headers,
                    self.poll_interval_seconds,
                    self.timeout_seconds,
                    context.log,
                    cancel_on_timeout=self.cancel_on_timeout,
                )
                context.log.info(f"meltano cloud job {job_id} completed: exitCode={final.get('exitCode')}")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
