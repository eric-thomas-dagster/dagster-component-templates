"""EventLogToDatadogJobComponent.

Op-shaped job that ships new Dagster event log entries to Datadog as
log entries via the HTTP intake API. Useful for teams whose obs stack
is Datadog — get Dagster reliability alongside your app + infra logs.

API: POST https://http-intake.logs.datadoghq.com/api/v2/logs (or the
EU / gov equivalents) with DD-API-KEY header.
"""

import json
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "event_log_to_datadog/last_storage_id"

DEFAULT_EVENT_TYPES = [
    dg.DagsterEventType.PIPELINE_START,
    dg.DagsterEventType.PIPELINE_SUCCESS,
    dg.DagsterEventType.PIPELINE_FAILURE,
    dg.DagsterEventType.STEP_FAILURE,
    dg.DagsterEventType.ASSET_MATERIALIZATION,
    dg.DagsterEventType.ASSET_OBSERVATION,
    dg.DagsterEventType.ASSET_CHECK_EVALUATION,
]

# Datadog log intake: hard cap of 5MB / 1000 entries per request.
DD_BATCH_MAX = 1000


def _ddlog(rec, service: str, env: str) -> Dict[str, Any]:
    e = rec.event_log_entry
    de = e.dagster_event
    return {
        "ddsource": "dagster",
        "service": service,
        "env": env,
        "ddtags": f"job_name:{e.job_name or 'unknown'},event_type:{de.event_type_value if de else 'unknown'}",
        "timestamp": int(e.timestamp * 1000),  # DD expects ms
        "message": e.message[:2000] if e.message else "",
        "attributes": {
            "storage_id": rec.storage_id,
            "run_id": e.run_id,
            "job_name": e.job_name,
            "event_type": de.event_type_value if de else None,
            "step_key": e.step_key,
            "asset_key": e.asset_key.to_user_string() if e.asset_key else None,
        },
    }


class EventLogToDatadogJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that ships new Dagster event log entries to
    Datadog logs intake."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/5 * * * *", description="Cron schedule; default every 5min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    api_key_env: str = Field(
        default="DATADOG_API_KEY",
        description="Env var with the Datadog API key (sent as DD-API-KEY).",
    )
    intake_url: str = Field(
        default="https://http-intake.logs.datadoghq.com/api/v2/logs",
        description="Datadog intake URL. EU: https://http-intake.logs.datadoghq.eu/api/v2/logs",
    )
    service: str = Field(default="dagster", description="`service` tag on emitted logs.")
    env: str = Field(default="prod", description="`env` tag on emitted logs.")

    initial_lookback_hours: int = Field(default=6, ge=1, description="On first run, pull events from last N hours.")
    batch_limit: int = Field(default=5000, ge=1, description="Max events per tick (split into 1000-entry sub-batches).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            import os
            import requests
            key = os.environ.get(_self.api_key_env)
            if not key:
                raise RuntimeError(f"Missing {_self.api_key_env} env var")

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

            headers = {"DD-API-KEY": key, "Content-Type": "application/json"}
            posted = 0
            for i in range(0, len(records), DD_BATCH_MAX):
                batch = [_ddlog(r, _self.service, _self.env) for r in records[i:i + DD_BATCH_MAX]]
                resp = requests.post(_self.intake_url, headers=headers, data=json.dumps(batch), timeout=30)
                resp.raise_for_status()
                posted += len(batch)

            new_cursor = records[-1].storage_id
            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(new_cursor)})
            context.log.info(f"Posted {posted} events to Datadog (service={_self.service}, env={_self.env})")
            return {"posted": posted, "last_storage_id": new_cursor}

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
