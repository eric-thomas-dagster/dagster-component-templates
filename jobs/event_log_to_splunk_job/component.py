"""EventLogToSplunkJobComponent.

Op-shaped job that ships new Dagster event log entries to Splunk via
the HTTP Event Collector (HEC). Complements `dagster_plus_to_siem_job`
(which is Dagster+ audit-log specific and multi-destination) — this
one is Dagster OSS event-log specific and Splunk-only, simpler shape.

Splunk HEC: POST https://<host>:8088/services/collector/event with
Authorization: Splunk <hec-token>.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "event_log_to_splunk/last_storage_id"

DEFAULT_EVENT_TYPES = [
    dg.DagsterEventType.PIPELINE_START,
    dg.DagsterEventType.PIPELINE_SUCCESS,
    dg.DagsterEventType.PIPELINE_FAILURE,
    dg.DagsterEventType.STEP_FAILURE,
    dg.DagsterEventType.ASSET_MATERIALIZATION,
    dg.DagsterEventType.ASSET_OBSERVATION,
    dg.DagsterEventType.ASSET_CHECK_EVALUATION,
]


def _hec_event(rec, sourcetype: str, index: Optional[str]) -> Dict[str, Any]:
    e = rec.event_log_entry
    de = e.dagster_event
    body: Dict[str, Any] = {
        "time": e.timestamp,
        "source": "dagster",
        "sourcetype": sourcetype,
        "event": {
            "storage_id": rec.storage_id,
            "run_id": e.run_id,
            "job_name": e.job_name,
            "event_type": de.event_type_value if de else None,
            "step_key": e.step_key,
            "asset_key": e.asset_key.to_user_string() if e.asset_key else None,
            "message": e.message[:2000] if e.message else None,
        },
    }
    if index:
        body["index"] = index
    return body


class EventLogToSplunkJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that ships new Dagster event log entries to
    Splunk via HEC."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/5 * * * *", description="Cron schedule; default every 5min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    hec_url: str = Field(
        default="https://splunk.example.com:8088/services/collector/event",
        description="Splunk HEC endpoint URL.",
    )
    hec_token_env: str = Field(
        default="SPLUNK_HEC_TOKEN",
        description="Env var holding the HEC token (sent as 'Authorization: Splunk <token>').",
    )
    sourcetype: str = Field(default="dagster:event", description="Splunk sourcetype for indexed events.")
    index: Optional[str] = Field(default=None, description="Optional Splunk index (falls back to HEC default).")
    verify_ssl: bool = Field(default=True, description="Verify TLS cert (False for self-signed dev Splunk).")

    initial_lookback_hours: int = Field(default=6, ge=1, description="On first run, pull events from last N hours.")
    batch_limit: int = Field(default=5000, ge=1, description="Max events per tick.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            import requests
            token = os.environ.get(_self.hec_token_env)
            if not token:
                raise RuntimeError(f"Missing {_self.hec_token_env} env var")

            last_storage_id = None
            try:
                prior = context.instance.get_runs(
                    filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                    limit=2,
                )
                for r in prior:
                    if r.run_id == context.run_id:
                        continue
                    v = (r.tags or {}).get(CURSOR_TAG)
                    if v:
                        last_storage_id = int(v)
                        break
            except Exception:
                pass

            filter_kwargs: Dict[str, Any] = {}
            if last_storage_id is not None:
                filter_kwargs["after_cursor"] = last_storage_id
            else:
                filter_kwargs["after_timestamp"] = time.time() - _self.initial_lookback_hours * 3600.0

            records: List[Any] = []
            for et in DEFAULT_EVENT_TYPES:
                try:
                    batch = context.instance.get_event_records(
                        event_records_filter=dg.EventRecordsFilter(event_type=et, **filter_kwargs),
                        limit=_self.batch_limit,
                        ascending=True,
                    )
                    records.extend(batch)
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"Failed to query {et}: {exc}")

            records.sort(key=lambda r: r.storage_id)
            records = records[:_self.batch_limit]

            if not records:
                context.log.info("No new events since last run.")
                context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(last_storage_id or 0)})
                return {"posted": 0, "last_storage_id": last_storage_id}

            headers = {"Authorization": f"Splunk {token}", "Content-Type": "application/json"}
            # HEC accepts newline-delimited events in a single POST body.
            body = "\n".join(json.dumps(_hec_event(r, _self.sourcetype, _self.index)) for r in records)
            resp = requests.post(
                _self.hec_url, headers=headers, data=body.encode("utf-8"),
                timeout=30, verify=_self.verify_ssl,
            )
            resp.raise_for_status()

            new_cursor = records[-1].storage_id
            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(new_cursor)})
            context.log.info(f"Posted {len(records)} events to Splunk HEC (sourcetype={_self.sourcetype})")
            return {"posted": len(records), "last_storage_id": new_cursor}

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _export()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)
