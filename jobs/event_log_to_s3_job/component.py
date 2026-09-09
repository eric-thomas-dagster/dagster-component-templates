"""EventLogToS3JobComponent.

Op-shaped job that reads new Dagster event log entries (since last
successful run) and writes them to S3 as gzipped JSONL batches keyed
by date/hour. Useful for cheap event archival, cold-storage compliance,
or downstream ingestion into a data lake.

Cursor is a storage_id, stored as a run tag between invocations. First
run pulls up to `initial_lookback_hours` of history; subsequent runs
pull only new events.
"""

import gzip
import io
import json
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "event_log_to_s3/last_storage_id"

# Event types to export. Restrict to the ones with real analytical value —
# skipping fine-grained step-lifecycle events keeps volume + noise down.
DEFAULT_EVENT_TYPES = [
    dg.DagsterEventType.PIPELINE_START,
    dg.DagsterEventType.PIPELINE_SUCCESS,
    dg.DagsterEventType.PIPELINE_FAILURE,
    dg.DagsterEventType.STEP_FAILURE,
    dg.DagsterEventType.ASSET_MATERIALIZATION,
    dg.DagsterEventType.ASSET_OBSERVATION,
    dg.DagsterEventType.ASSET_CHECK_EVALUATION,
]


def _serialize_event(rec) -> Dict[str, Any]:
    e = rec.event_log_entry
    de = e.dagster_event
    return {
        "storage_id": rec.storage_id,
        "timestamp": e.timestamp,
        "run_id": e.run_id,
        "job_name": (getattr(de, "job_name", None) if de else None) or e.job_name,
        "event_type": de.event_type_value if de else None,
        "step_key": e.step_key,
        "asset_key": e.asset_key.to_user_string() if e.asset_key else None,
        "message": e.message[:2000] if e.message else None,
    }


class EventLogToS3JobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that streams new Dagster event log entries to S3
    as gzipped JSONL batches."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/15 * * * *", description="Cron schedule; default every 15min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    bucket: str = Field(description="S3 bucket name.")
    key_prefix: str = Field(default="dagster-events/", description="Object key prefix; date/hour partition appended.")
    aws_region: Optional[str] = Field(default=None, description="Optional AWS region (falls back to boto3 default chain).")
    initial_lookback_hours: int = Field(default=24, ge=1, description="On first run, pull events from last N hours.")
    batch_limit: int = Field(default=5000, ge=1, description="Max events per tick.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            import boto3
            s3 = boto3.client("s3", region_name=_self.aws_region) if _self.aws_region else boto3.client("s3")

            # Resolve cursor from prior successful run.
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

            # Query new events.
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
                return {"uploaded": 0, "last_storage_id": last_storage_id}

            # Batch by hour bucket for partitioned S3 layout.
            batches: Dict[str, List[dict]] = {}
            for rec in records:
                d = _serialize_event(rec)
                ts = time.gmtime(d["timestamp"])
                hour_key = time.strftime("%Y-%m-%d/%H", ts)
                batches.setdefault(hour_key, []).append(d)

            uploaded = 0
            for hour_key, events in batches.items():
                buf = io.BytesIO()
                with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                    for e in events:
                        gz.write((json.dumps(e) + "\n").encode("utf-8"))
                buf.seek(0)
                key = f"{_self.key_prefix.rstrip('/')}/{hour_key}/events-{int(time.time())}-{context.run_id[:8]}.jsonl.gz"
                s3.put_object(Bucket=_self.bucket, Key=key, Body=buf.getvalue(), ContentType="application/gzip")
                uploaded += len(events)
                context.log.info(f"s3://{_self.bucket}/{key} — {len(events)} events")

            new_cursor = records[-1].storage_id
            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(new_cursor)})
            return {
                "uploaded": uploaded,
                "batches": len(batches),
                "last_storage_id": new_cursor,
            }

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
