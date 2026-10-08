"""StitchSyncTriggerJobComponent.

Trigger a Stitch (Talend's -- now Qlik's, post-2023-acquisition -- managed
ELT product) replication job via the Stitch Connect API, and optionally poll
it to completion.

This is distinct from `talend_cloud_workspace`, which drives Talend Cloud's
own Data Fabric jobs/pipelines -- a different product with a different API.
Stitch has always been operated as a separate product/brand even through
two ownership changes (Talend -> Qlik's 2023 Talend acquisition), and its
Connect API is still live and documented today (confirmed 2026-10 against
https://www.stitchdata.com/docs/developers/stitch-connect/api -- not a stale
blog post): Bearer-token auth, `POST /v4/sources/{source_id}/sync` to start
a replication job, `DELETE /v4/sources/{source_id}/sync` to stop one, and
`GET /v4/{client_id}/extractions` to list recent extraction job history for
an account (rate-limited to 30 requests / 10 minutes).

Caveat (be honest about this, don't paper over it): Stitch's current public
docs describe the extraction-job *resource* only at a high level and do not
spell out its full field schema the way Airbyte's or Meltano Cloud's job
objects are documented. There is no single "sync run id" returned by the
trigger call to poll directly. So `wait_for_completion` here is inherently
best-effort: it re-lists `/v4/{client_id}/extractions` after triggering and
matches the most recent record for `source_id`, checking several plausible
field-name variants (`job_finished_at`/`end_time`/`finished_at` for
completion, `tap_exit_status`/`extractor_exit_status` and
`target_exit_status`/`loader_exit_status` for success/failure) rather than
assuming one exact shape. `wait_for_completion` defaults to False for this
reason -- verify the real field names against your own account's response
before relying on it in production. Triggering itself (the documented,
simple part) is solid.
"""

import time
from typing import Any, Dict, List, Optional

import dagster as dg
import requests
from pydantic import Field

_START_FIELD_CANDIDATES = ("job_started_at", "start_time", "started_at")
_END_FIELD_CANDIDATES = ("job_finished_at", "end_time", "finished_at")
_TAP_EXIT_CANDIDATES = ("tap_exit_status", "extractor_exit_status")
_TARGET_EXIT_CANDIDATES = ("target_exit_status", "loader_exit_status")


def _auth_headers(token_env: str) -> Dict[str, str]:
    import os

    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _trigger_sync(base_url: str, source_id: str, headers: Dict[str, str]) -> Dict[str, Any]:
    r = requests.post(f"{base_url}/v4/sources/{source_id}/sync", headers=headers, timeout=60)
    if r.status_code >= 300:
        raise Exception(f"stitch sync trigger failed: {r.status_code} {r.text[:200]}")
    try:
        return r.json()
    except Exception:
        return {}


def _list_extractions(base_url: str, client_id: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    r = requests.get(f"{base_url}/v4/{client_id}/extractions", headers=headers, timeout=30)
    if r.status_code >= 300:
        raise Exception(f"stitch extractions list failed: {r.status_code} {r.text[:200]}")
    body = r.json()
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "extractions", "results"):
            if isinstance(body.get(key), list):
                return body[key]
    return []


def _first_present(record: Dict[str, Any], keys) -> Any:
    for k in keys:
        if k in record and record[k] is not None:
            return record[k]
    return None


def _matches_source(record: Dict[str, Any], source_id: str) -> bool:
    candidate = record.get("source_id", record.get("integration_id"))
    return candidate is not None and str(candidate) == str(source_id)


def _is_finished(record: Dict[str, Any]) -> bool:
    return _first_present(record, _END_FIELD_CANDIDATES) is not None


def _is_successful(record: Dict[str, Any]) -> bool:
    tap = _first_present(record, _TAP_EXIT_CANDIDATES)
    target = _first_present(record, _TARGET_EXIT_CANDIDATES)
    if tap is None and target is None:
        # Neither exit-status field is present -- Stitch's public docs don't
        # guarantee one, so "finished" is the clearest signal we have.
        return True
    return tap in (0, None) and target in (0, None)


def _find_latest_matching_extraction(records: List[Dict[str, Any]], source_id: str) -> Optional[Dict[str, Any]]:
    candidates = [r for r in records if _matches_source(r, source_id)]
    if not candidates:
        return None
    candidates.sort(key=lambda r: _first_present(r, _START_FIELD_CANDIDATES) or "", reverse=True)
    return candidates[0]


def _poll_until_terminal(
    base_url: str,
    client_id: str,
    source_id: str,
    headers: Dict[str, str],
    poll_interval_seconds: int,
    timeout_seconds: int,
    log,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        records = _list_extractions(base_url, client_id, headers)
        record = _find_latest_matching_extraction(records, source_id)
        if record is not None and _is_finished(record):
            if not _is_successful(record):
                raise Exception(f"stitch extraction for source {source_id} failed: {record}")
            return record
        log.info(f"poll: stitch source={source_id} extraction not yet finished")
        time.sleep(poll_interval_seconds)
    raise Exception(f"timed out waiting for stitch extraction on source {source_id}")


class StitchSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Stitch (Talend/Qlik) replication job via the Stitch Connect API."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    stitch_api_url: str = Field(default="https://api.stitchdata.com", description="Stitch Connect API base URL")
    client_id: str = Field(description="Stitch client ID (stitch_client_id), used to list extraction history for polling")
    source_id: str = Field(description="Stitch source/integration ID to sync")
    api_token_env: str = Field(
        default="STITCH_API_TOKEN",
        description="Env var holding the Stitch Connect API access token (Bearer; generated in Stitch account settings, does not expire)",
    )
    wait_for_completion: bool = Field(
        default=False,
        description="Poll Stitch's extraction history until this source's sync finishes. Best-effort -- see module docstring; Stitch's API gives no single run-id to poll directly.",
    )
    poll_interval_seconds: int = Field(
        default=30,
        description="Seconds between polls. Stitch rate-limits extraction endpoints to 30 requests/10 minutes -- keep this >= 20",
    )
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait before giving up")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            headers = _auth_headers(self.api_token_env)
            _trigger_sync(self.stitch_api_url, self.source_id, headers)
            context.log.info(f"triggered stitch sync for source {self.source_id}")
            if self.wait_for_completion:
                record = _poll_until_terminal(
                    self.stitch_api_url,
                    self.client_id,
                    self.source_id,
                    headers,
                    self.poll_interval_seconds,
                    self.timeout_seconds,
                    context.log,
                )
                context.log.info(f"stitch extraction finished for source {self.source_id}: {record}")

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
